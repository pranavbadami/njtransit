import json
from rail_data import dv_station_names as dv
import pandas as pd
import re
from datetime import datetime, timedelta
import boto3
import os
from os.path import isfile, join

s3 = boto3.resource('s3')

ALL_STATIONS = dv.ALL_STATIONS
TIME_LEN = len("YYYY-MM-DD HH:MM:SS")
DAY_LEN = len("YYYY-MM-DD ")
RAIL_DATA = "./rail_data/"

trip_stops = pd.DataFrame()
trips = pd.read_csv(RAIL_DATA + 'trips.txt')
stop_times = pd.read_csv(RAIL_DATA + 'stop_times.txt')
trip_stops = stop_times.merge(trips, on=['trip_id'])
trip_stops.rename(columns={'arrival_time': 'expected'}, inplace=True)

class TrainParser:
	time_re = re.compile(".*?(\d+):(\d+).*")

	def __init__(self, filename):
		self.filename = filename
		self.data = self.read(self.filename)
		self.train = self.data['id']
		self.line = self.data['line']
		self.type = self.data['type']
		self.scheduled = self.data['scheduled']
		self.created_at = self.data['created_at']

	def read(self, filename):
		try:
			return json.load(open(filename))
		except ValueError:
			print "error reading {}".format(filename)
			return None

	def parse_time(self, hour, minute, t_s):
		hour, minute = int(hour), int(minute)
		t_s = t_s[:TIME_LEN]
		t_s = datetime.strptime(t_s, "%Y-%m-%d %H:%M:%S")

		t_dep = datetime(year=t_s.year, month=t_s.month, day=t_s.day, hour=hour, minute=minute)
		t_dep_eve = t_dep + timedelta(hours=12)
		t_dep_nxt = t_dep + timedelta(days=1)

		diff_dep = abs((t_s - t_dep).total_seconds())
		diff_dep_eve = abs((t_s - t_dep_eve).total_seconds())
		diff_dep_nxt = abs((t_s - t_dep_nxt).total_seconds())

		diffs = [diff_dep, diff_dep_eve, diff_dep_nxt]
		min_diff = diffs.index(min(diffs))
		if min_diff == 0:
			return t_dep.strftime("%Y-%m-%d %H:%M:%S")
		elif min_diff == 1:
			return t_dep_eve.strftime("%Y-%m-%d %H:%M:%S")
		else:
			return t_dep_nxt.strftime("%Y-%m-%d %H:%M:%S")

	def get_stop_times(self):
		dep_count = 0
		departures = []
		finished_stations = {}
		for frame in self.data['data']:
			time = frame[0]
			stops = frame[1]
			for idx, stop in enumerate(stops):
				try:
					station, status = stop.split(u"\xa0\xa0")
				except ValueError:
					station = ""
					status = ""
				if station in finished_stations:
					pass
				elif ("DEPARTED" in status):
					if station in ALL_STATIONS:
						departures.append({'station': station,
										   'time': time,
										   'status': "Departed"})
						finished_stations[station] = True

				elif ("Cancelled" in status):
					if station in ALL_STATIONS:
						departures.append({'station': station,
									   'time': time,
									   'status': "Cancelled"})
						finished_stations[station] = True

		if self.type == "NJ Transit":
			if (len(departures) + 1) < len(self.data['data'][0][1]):
				# print len(departures)
				for stop in self.data['data'][-2][1][len(departures):]:
					try:
						station, status = stop.split(u"\xa0\xa0")
					except ValueError:
						station = ""
						status = ""
					if station in ALL_STATIONS:
						# print "station", station, status
						match = self.time_re.match(status)
						approx_time = self.parse_time(match.group(1), match.group(2), time)

						departures.append({'station': station,
										   'time': approx_time,
										   'status': None})
			# time prediction of last station from penultimate frame
			try:
				penultimate = self.data['data'][-2]
				scrape_time = penultimate[0]
				stop = penultimate[1][-2]
				station, status = stop.split(u"\xa0\xa0")
				if station in ALL_STATIONS:
					match = self.time_re.match(status)
					approx_time = self.parse_time(match.group(1), match.group(2), scrape_time)
					departures[-1] = {'station': departures[-1]['station'],
									  'time': approx_time,
									  'status': departures[-1]['status']}
			except IndexError:
				pass
			except AttributeError:
				pass

		return departures

	def get_rows(self, departures):
		if not len(departures):
			return []
		rows = []
		prev = departures[0]
		for idx, departure in enumerate(departures):
			row = {
				   "stop_num": idx + 1, 
				   "train_id": self.train,
				   "line": self.line,
				   "type":self.type,
				   "scheduled": self.scheduled,
				   "from": prev['station'],
				   "to": departure['station'],
				   "time": departure['time'][:TIME_LEN],
				   "status": departure['status']
				   }
			rows.append(row)
			prev = departure
		return rows

	def get_df(self, rows):
		if not len(rows):
			return None
		df = pd.DataFrame(rows)
		df.set_index("stop_num", inplace=True)
		return df

	def format_schedule_time(self, scheduled):
		date, time = scheduled.split(" ")
		hours, minutes, seconds = time.split(":")
		hours, minutes, seconds = int(hours), int(minutes), int(seconds)
		midnight = datetime.strptime(date, "%Y-%m-%d")
		actual_time = midnight + timedelta(hours=hours, minutes=minutes)
		return actual_time.strftime("%Y-%m-%d %H:%M:%S")
	
	def join_schedule(self, df):
		if df is None:
			return None
		num_stops = len(df)
		if self.scheduled:
			stops = trip_stops[trip_stops['block_id'] == self.train]
			trip_ids = stops.groupby("trip_id").size()
			valid_ids = trip_ids[trip_ids == num_stops]
			if not len(valid_ids):
				print "scheduling error", self.filename
				return None
			stops = stops[stops['trip_id'] == valid_ids.index.unique()[0]]
			stops = stops[['expected', 'stop_sequence']]
			stops.set_index('stop_sequence', inplace=True)
			stops['expected'] = self.created_at[:DAY_LEN] + stops['expected']
			stops['expected'] = stops['expected'].apply(lambda x: self.format_schedule_time(x))
			return df.join(stops)
		else:
			df['expected'] = None
			df['stop_sequence'] = None
			return df

	def is_valid(self, df):
		if df is None:
			return False
		num_stops = len(df)
		return ((df['from'].nunique() + 1) == num_stops) & (df['to'].nunique() == num_stops)
 
# parses all trains in a directory and outputs a CSV
class DayParser:

	def __init__(self, path, day, csv_path='./csv/'):
		self.path = path + day + '/'
		self.day = day
		self.csv_path = csv_path
		self.files = [f for f in os.listdir(self.path) if not f.startswith(".")]
		self.invalid_trains = []

	def parse_train(self, filename):
		train_name = self.path + filename
		t = TrainParser(train_name)
		times = t.get_stop_times()
		rows = t.get_rows(times)
		df = t.get_df(rows)
		performance = t.join_schedule(df)
		if not t.is_valid(performance):
			return None
		else:
			return performance

	def parse_all_trains(self):
		all_trains = None
		count = 0
		for train in self.files:
			print train
			train_df = self.parse_train(train)
			if train_df is not None:
				count = count + 1
				if all_trains is None:
					all_trains = train_df
				else:
					train_df.reset_index(inplace=True)
					all_trains = all_trains.append(train_df, ignore_index=True)
			else:
				self.invalid_trains.append(train)
		all_trains.to_csv('{}.csv'.format(self.day))
		print "successfully parsed", count, "trains to", '{}.csv'.format(self.day)
		print len(self.invalid_trains), "invalid trains"
		print self.invalid_trains



#prefix = rpi/
def download_train_files(year, month, day, path='./scraped_data/', prefix=''):
	year, month, day = str(year), str(month), str(day)
	month = month.zfill(2)
	day = day.zfill(2)
	day_str = '{}_{}_{}/'.format(year, month, day)
	directory = path + day_str
	if not os.path.exists(directory):
		os.makedirs(directory)
	bucket = s3.Bucket('njtransit')
	for obj in bucket.objects.filter(Prefix=prefix+day_str).limit(10):
		with open(path + obj.key, 'a') as outfile:
			s3_obj = obj.get()
			data = s3_obj['Body'].read()
			outfile.write(data)
			outfile.close()
	return obj






