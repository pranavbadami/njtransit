import json
import pandas as pd
import re
from datetime import datetime, timedelta
import boto3
import os
from os.path import isfile, join

s3 = boto3.resource('s3')

TIME_LEN = len("YYYY-MM-DD HH:MM:SS")
DAY_LEN = len("YYYY-MM-DD ")
RAIL_DATA = "./rail_data/"
ALL_STATIONS = json.load(open(RAIL_DATA + 'rail_stations'))
BUCKET = "njtransit"

trip_stops = pd.DataFrame()
trips = pd.read_csv(RAIL_DATA + 'trips.txt')
stop_times = pd.read_csv(RAIL_DATA + 'stop_times.txt')
trip_stops = stop_times.merge(trips, on=['trip_id'])
trip_stops.rename(columns={'arrival_time': 'expected'}, inplace=True)

class TrainParser:
	time_re = re.compile(".*?(\d+):(\d+).*")
	departed_statuses = ["Departed", "DEPARTED", "departed"]
	cancelled_statuses = ["Cancelled", "CANCELLED", "cancelled"]

	def __init__(self, filename):
		self.filename = filename
		self.data = self.read_file(self.filename)
		self.train = self.data['id']
		self.line = self.data['line']
		self.type = self.data['type']
		self.scheduled = self.data['scheduled']
		self.created_at = self.data['created_at']
		self.created_dt = datetime.strptime(self.created_at, "%Y-%m-%d %H:%M:%S.%f")
		self.departures = []
		self.departed_stations = {}

	def read_file(self, filename):
		try:
			return json.load(open(filename))
		except ValueError:
			contents = open(filename).read().split('}{')
			return json.loads(contents[0] + '}')

	def parse_time(self, hour, minute, t_s):
		hour, minute = int(hour), int(minute)
		t_s = t_s[:TIME_LEN]
		t_s = datetime.strptime(t_s, "%Y-%m-%d %H:%M:%S")

		t_dep = datetime(year=self.created_dt.year, month=self.created_dt.month,
						 day=self.created_dt.day, hour=hour, minute=minute)
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

	def parse_station_and_status(self, stop):
		try:
			station, status = stop.split(u"\xa0\xa0")
		except ValueError:
			station = ""
			status = ""
		return station, status

	def update_departure(self, new_departure):
		station = new_departure['station']
		departure_idx = self.departed_stations[station]
		prev_departure_status = self.departures[departure_idx]['status']
		if prev_departure_status in self.departed_statuses:
			if new_departure['status'] in self.cancelled_statuses:
				self.departures[departure_idx].update(new_departure)

	def append_departure(self, departed_stop):
		station = departed_stop['station']
		self.departures.append(departed_stop)
		self.finished_stations[station] = len(self.departures) - 1

	def append_or_update_departure(self, departed_stop):
		station = departed_stop['station']
		if station in self.departed_stations:
			self.update_departure(dep_idx, departed_stop)

		elif station in ALL_STATIONS:
			self.append_departure()

	def parse_status_line_if_departed(self, line):
		station, status = self.parse_station_and_status(line)
		if (status in self.departed_statuses) or (status in self.cancelled_statuses):
			return {'station': station,
					'status': status.lower()}
		else:
			return None
		
	def parse_departures_from_status_page(self, page):
		time_scraped = page[0]
		page_lines = page[1]

		for idx, line in enumerate(page_lines):
			departed_stop = self.parse_status_line_if_departed(line)
			if departed_stop is not None:
				departed_stop['time_scraped'] = time_scraped
				self.append_or_update_departure(departed_stop)

	#TODO: more descriptive name
	def get_stop_times(self):
		dep_count = 0
		departures = []
		finished_stations = {}
		len_stops = 0
		### currently being refactored
		for page in self.data['data']:
			parse_departures_from_status_page(page)
			time = frame[0]
			stops = frame[1]
			# TODO: what is this for??
			if len(stops) > len_stops:
				len_stops = len(stops)
			# END TODO
			for idx, stop in enumerate(stops):
				station, status = self.parse_station(stop)

				if station in finished_stations:
					dep_idx = finished_stations[station]
					departures[dep_idx] = self.update_stop_times(status, time, departures[dep_idx])

				elif ("DEPARTED" in status):
					if station in ALL_STATIONS:
						departures.append({'station': station,
										   'time': time,
										   'status': "Departed"})
						finished_stations[station] = len(departures) - 1

				elif ("Cancelled" in status) or ("CANCELLED" in status):
					if station in ALL_STATIONS:
						departures.append({'station': station,
									   'time': time,
									   'status': "Cancelled"})
						finished_stations[station] = len(departures) - 1
		### end currently being refactored
			if len(departures) == len(stops):
				break


		if self.type == "NJ Transit":
			# fill in estimated predictions
			if (len(departures) + 1) < len_stops:
				error = False
				# print len(departures)
				if len(self.data['data']) > 1:
					for stop in self.data['data'][-3][1][len(departures):]:
						station, status = self.parse_station(stop)
						if station in ALL_STATIONS:
							# print "station", station, status
							match = self.time_re.match(status)
							if match is not None:
								approx_time = self.parse_time(match.group(1), match.group(2), time)

								departures.append({'station': station,
												   'time': approx_time,
												   'status': None})
			# time prediction of last station from penultimate frame
			try:
				penultimate = self.data['data'][-3]
				scrape_time = penultimate[0]
				stop = penultimate[1][-2]
				station, status = self.parse_station(stop)
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

		# comb through departures and cancel as needed
		cancelled = False
		cancel_time = None
		for departure in departures:
			if not cancelled and (departure['status'] == 'Cancelled'):
				cancelled = True
				cancel_time = departure['time']
			if cancelled:
				departure['status'] = 'Cancelled'
				departure['time'] = cancel_time
		return departures

	def get_rows(self, departures):
		if not len(departures):
			return []
		rows = []
		prev = departures[0]
		for idx, departure in enumerate(departures):
			row = {
				   "train_id": self.train,
				   "line": self.line,
				   "type":self.type,
				   "scheduled": self.scheduled,
				   "from": prev['station'],
				   "from_id": ALL_STATIONS[prev['station']],
				   "to": departure['station'],
				   "to_id": ALL_STATIONS[departure['station']],
				   "time": departure['time'][:TIME_LEN],
				   "status": departure['status'],
				   "date": self.created_at[:DAY_LEN-1]
				   }
			rows.append(row)
			prev = departure
		return rows

	def get_df(self, rows):
		if not len(rows):
			return None
		df = pd.DataFrame(rows)
		return df

	def parse_file_to_df(self):
		times = self.get_stop_times()
		rows = self.get_rows(times)
		return self.get_df(rows)

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
			stops = trip_stops[trip_stops['block_id'] == self.train].copy()
			stops.drop_duplicates(subset='stop_id', inplace=True)
			stops = stops[['expected', 'stop_sequence', 'stop_id']]
			stops['expected'] = self.created_at[:DAY_LEN] + stops['expected']
			stops['expected'] = stops['expected'].apply(lambda x: self.format_schedule_time(x))
			return df.merge(stops, left_on='to_id', right_on='stop_id', how='left')
		else:
			df['expected'] = None
			df['stop_sequence'] = None
			return df

	def is_valid(self, df):
		if df is None:
			return False
		num_stops = len(df)
		return ((df['from'].nunique() + 1) == num_stops) & (df['to'].nunique() == num_stops)
 
class DayParser:
	"""Parses train files in a directory, which correspond to a day.

    For a given day, DayParser can parse all train files in the day's directory 
    to a combined DataFrame containing stop-level data for all trains that day.
    """

    #TODO: change column names as needed
	df_columns = ['train_id', 'date', 'stop_sequence', 'from', 'from_id', 'to',
			   'to_id', 'expected', 'time', 'status', 'line', 'type']

	def __init__(self, path, day, csv_path='./csv/'):
		self.path = path + day + '/'
		self.day = day
		self.csv_path = csv_path
		self.files = [f for f in os.listdir(self.path) if not f.startswith(".")]
		self.all_trains_df = pd.DataFrame(columns=self.df_columns)
		self.invalid_trains = []
	
	def parse_train(self, filename):
		"""
		Parse data in a train file to a DataFrame where each row represents a
		pair of stops along journey. 
		
		Creates a TrainParser instance from a train file, parses file data to
		DataFrame, joins schedule data to dataframe, and returns DataFrame.
		"""
		train_filename = self.path + filename
		train = TrainParser(train_filename)
		train_df = train.parse_file_to_df()
		performance = train.join_schedule(train_df)
		if not train.is_valid(performance):
			return None
		else:
			return performance[self.df_columns]

	def parse_all_trains(self):
		"""Parse all train files in directory to dataframe (self.all_trains_df).
		If unable to parse train file, store train id (self.invalid_trains).
		"""
		all_trains = []
		for train in self.files:
			train_df = self.parse_train(train)
			if train_df is not None:
				all_trains.append(train_df)
			else:
				self.invalid_trains.append(train)
		self.all_trains_df = pd.concat(all_trains, ignore_index=True)

	def write_day_to_disk(self, print_results=True):
		"""Write dataframe of vaid parsed trains to disk.

		Keyword arguments:
		print_results --- if True, parse counts are printed to stdout
		"""
		self.all_trains_df.to_csv('{}{}.csv'.format(self.csv_path, self.day),
								  index=False)
		if print_results:
			self.print_results()

	def get_parsed_counts(self):
		"""Count valid, invalid and total trains for day. """
		count_valid = self.all_trains_df['train_id'].nunique()
		count_invalid = len(self.invalid_trains)
		return {
			"valid": count_valid,
			"invalid": count_invalid,
			"total": count_valid + count_invalid
		}

	def print_results(self):
		"""Print out number of successfully and unsucessfully (invalid) parsed
		trains for day.
		"""
		counts = self.get_parsed_counts()
		print("successfully parsed", counts['valid'], "trains for", self.day)
		print(len(self.invalid_trains), "invalid trains for", self.day)
		print(self.invalid_trains)


################################################################################
# HELPER METHODS
#
# Functions to use in Python shell or script to download data and instantiate
# Parser objects.
################################################################################

def create_directory(dir_name, path='./scraped_data/'):
	"""Create a directory at path + dir_name + "/". """

	directory = path + dir_name + '/'
	if not os.path.exists(directory):
		os.makedirs(directory)
	return directory

def write_s3_obj_to_disk(s3_obj, directory):
	"""Read s3_obj data, get filename, and write to path + filename.

	Keyword arguments:
	s3_obj -- object from s3 bucket
	directory -- relative folder path to write file data
	"""
	filename = s3_obj.key.split("/")[-1]
	data = s3_obj.get()['Body'].read()
	outfile = open(directory + filename, 'a')
	outfile.write(data)
	outfile.close()


def download_train_files(date_string, path='./scraped_data/', prefix=''):
	
	"""Download files from bucket/prefix/date_string, write files to disk.

	Keyword arguments:
	date_string -- date prefix to group train files by day on S3 ('YYYY_MM_DD')
	path -- relative folder path where scraped data is stored
	prefix -- S3 prefix where train files are stored
	"""
	directory = create_directory(date_string, path)
	bucket = s3.Bucket(BUCKET)
	for obj in bucket.objects.filter(Prefix=prefix+date_string+'/'):
		write_s3_obj_to_disk(obj, directory)


def download_and_parse_days(days, path='./scraped_data/', prefix=''):
	"""Download and parse train files for days.

	Keyword arguments:
	days -- list of date strings, e.g. ['2018-03-01', '2018-03-02', ...]
	path -- relative folder path where scraped data is stored
	prefix -- S3 prefix where train files are stored
	"""
	for date_string in days:
		download_train_files(date_string, path, prefix)
		d = DayParser(path, date_string)
		d.parse_all_trains()
		d.write_day_to_disk()
		print("completed {}".format(date_string))

