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

TRIPS = pd.read_csv(RAIL_DATA + 'trips.txt')
STOP_TIMES = pd.read_csv(RAIL_DATA + 'stop_times.txt')
TRIP_STOPS = STOP_TIMES.merge(TRIPS, on=['trip_id'])
TRIP_STOPS.rename(columns={'arrival_time': 'expected'}, inplace=True)

class TrainParser:
	time_re = re.compile(".*?(\d+):(\d+).*")
	departed_statuses = ["Departed", "DEPARTED", "departed"]
	cancelled_statuses = ["Cancelled", "CANCELLED", "cancelled"]
	minimum_number_statuses = 3

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
		self.num_lines_parsed = 0
		self.corrupted = False
		self.pages_parsed = 0

	def read_file(self, filename):
		try:
			return json.load(open(filename))
		except ValueError:
			contents = open(filename).read().split('}{')
			return json.loads(contents[0] + '}')

	def check_page_valid(self, page):
		# len 3 --> two stops + empty string
		if len(page) < 3:
			return False
		return True

	def check_file_empty(self):
		first_status_page = self.data['data'][0][1]
		if not self.check_page_valid(first_status_page):
			self.corrupted = True
			print(self.filename, "corrupted empty")
			return True
		return False

	def get_number_status_lines(self):
		num_status_lines = 0
		i = 1
		while num_status_lines <= 1 and i <= len(self.data['data']):
			num_status_lines = len(self.data['data'][-i][1]) - 1
			i = i + 1
		return num_status_lines

	def parse_time_from_status(self, status):
		return self.time_re.match(status)
		
	def get_possible_departure_times(self, time_scraped, status):
		match = self.parse_time_from_status(status)
		if match is None:
			return None
		hour, minute = int(match.group(1)), int(match.group(2))
		dep_am = datetime(year=time_scraped.year, month=time_scraped.month,
						  day=time_scraped.day, hour=hour, minute=minute)
		return {
			"am": dep_am,
			"pm": dep_am + timedelta(hours=12),
			"next_day": dep_am + timedelta(days=1)
		}

	def get_most_likely_departure_time(self, departure_times, time_scraped):
		departure_times_keys = list(departure_times.keys())
		differences = [abs((time_scraped - departure_times[key]).total_seconds()) for key in departure_times_keys]
		min_idx = differences.index(min(differences))
		return departure_times[departure_times_keys[min_idx]].strftime("%Y-%m-%d %H:%M:%S")

	def approximate_time(self, status, time_scraped):		
		time_scraped = datetime.strptime(time_scraped[:TIME_LEN], "%Y-%m-%d %H:%M:%S")
		possible_departure_times = self.get_possible_departure_times(time_scraped, status)
		if possible_departure_times:
			approx_time = self.get_most_likely_departure_time(possible_departure_times,
															  time_scraped)
			return approx_time
		return None

	def parse_station_and_status(self, stop):
		try:
			station, status = stop.split(u"\xa0\xa0")
		except ValueError:
			station = ""
			status = ""
		return station, status

	def replace_departure(self, departed_stop):
		station = departed_stop['station']
		departure_idx = self.departed_stations[station]
		self.departures[departure_idx] = departed_stop

	def update_departure(self, new_departure):
		station = new_departure['station']
		if station in self.departed_stations:
			departure_idx = self.departed_stations[station]
			prev_departure_status = self.departures[departure_idx]['status']
			if prev_departure_status != new_departure['status']:
				self.departures[departure_idx].update(new_departure)
				return "updated"
			return "no change"
		return "outside station"

	def append_departure(self, departed_stop):
		station = departed_stop['station']
		if station in ALL_STATIONS:
			station = departed_stop['station']
			self.departures.append(departed_stop)
			self.departed_stations[station] = len(self.departures) - 1
			return "appended"
		return "outside station"

	def append_or_update_departure(self, departed_stop):
		station = departed_stop['station']
		if station in self.departed_stations:
			self.update_departure(departed_stop)
			return "updated"
		elif station in ALL_STATIONS:
			self.append_departure(departed_stop)
			return "appended"
		else:
			return "outside station" # not a station we care about

	def parse_status_line_if_departed(self, line):
		station, status = self.parse_station_and_status(line)
		if any(x in status for x in self.departed_statuses):
			status = "departed"
		elif any(x in status for x in self.cancelled_statuses):
			status = "cancelled"
		else:
			return None
		return {'station': station,
				'status': status}

	def parse_status_line_not_departed(self, line):
		station, status = self.parse_station_and_status(line)
		return {'station': station,
				'status': status}

	def get_relevant_stations(self, page):
		relevant_stations = []
		for line in page:
			station, status = self.parse_station_and_status(line)
			if station in ALL_STATIONS:
				relevant_stations.append(line)
		return relevant_stations

	def parse_departures_from_status_page(self, page):
		time_scraped = page[0]
		page_lines = page[1]

		if not self.check_page_valid(page_lines):
			return

		page_lines = self.get_relevant_stations(page_lines)

		#update SM
		for idx, line in enumerate(page_lines):
			if self.num_lines_parsed <= idx:
				break
			print("update", idx, line, len(self.departures))
			departed_stop = self.parse_status_line_if_departed(line)
			if departed_stop:
				updated = self.update_departure(departed_stop)
				if updated == "updated":
					# departure status changed, revise state
					self.num_lines_parsed = idx + 1
			else:
				# marked as not departed, revise state
				self.num_lines_parsed = idx
		self.departures = self.departures[:self.num_lines_parsed]

		#check SM
		try:

			for idx, line in enumerate(page_lines[self.num_lines_parsed:]):
				print("check", idx, line)
				# next_line = page_lines[len(self.departures)]
				departed_stop = self.parse_status_line_if_departed(line)
				if departed_stop is not None:
					self.num_lines_parsed = self.num_lines_parsed + 1
					departed_stop['time'] = time_scraped
					status = self.append_departure(departed_stop)
				else:
					break
		except IndexError:
			pass
		# for idx, line in enumerate(page_lines):
		# 	departed_stop = self.parse_status_line_if_departed(line)
		# 	if departed_stop is not None:
		# 		if idx > len(self.departures):
		# 			print(self.filename, "corrupted at ", idx, len(self.departures))
		# 			self.corrupted = True
		# 			break
		# 		departed_stop['time'] = time_scraped
		# 		self.append_or_update_departure(departed_stop)

	def get_last_page_with_time_estimate(self, line_idx):
		time_in_line = False
		i = 0
		while not time_in_line and i < len(self.data['data']):
			i = i + 1
			try:
				line = self.data['data'][-i][1][line_idx]
				time_in_line = ":" in line
			except IndexError:
				pass
		return -i

	def estimate_departure(self, line_idx):
		last_page = self.get_last_page_with_time_estimate(line_idx)
		time_scraped = self.data['data'][last_page][0]
		missing_line = self.data['data'][last_page][1][line_idx]
		estimated_stop = self.parse_status_line_not_departed(missing_line)
		approx_time = self.approximate_time(estimated_stop['status'], time_scraped)
		if approx_time is not None:
			estimated_stop['time'] = approx_time
			estimated_stop['status'] = 'departed'
			return estimated_stop
		return None

	def fill_missing_departures_with_estimates(self):
		num_status_lines = self.get_number_status_lines()
		num_departures = len(self.departures)
		for i in range(num_departures, num_status_lines):
			estimated_departure = self.estimate_departure(i)
			if estimated_departure is not None:
				self.append_or_update_departure(estimated_departure)

	def estimate_last_departure(self):
		num_status_lines = self.get_number_status_lines()
		estimated_departure = self.estimate_departure(num_status_lines-1)
		if estimated_departure is not None:
			if estimated_departure['station'] in self.departed_stations:
				self.replace_departure(estimated_departure)
			else:
				self.append_departure(estimated_departure)

	def update_departures_if_cancelled(self):
		train_cancelled = False
		cancel_time = None
		for departure in self.departures:
			if not train_cancelled and (departure['status'] == 'cancelled'):
				train_cancelled = True
				cancel_time = departure['time']
			elif train_cancelled:
				#TODO: do this idiomatically
				departure['status'] = 'cancelled'
				departure['time'] = cancel_time
				self.update_departure(departure)


	#TODO: more descriptive name
	def get_stop_times(self):
		for page in self.data['data']:
			self.parse_departures_from_status_page(page)
			self.pages_parsed = self.pages_parsed + 1
			if self.corrupted:
				break
		if not self.corrupted:
			if self.type == "NJ Transit":
				self.fill_missing_departures_with_estimates()
				self.estimate_last_departure()
			self.update_departures_if_cancelled()
		else:
			self.departures = []

	def get_rows(self):
		if not len(self.departures):
			return []
		rows = []
		prev = self.departures[0]
		for idx, departure in enumerate(self.departures):
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
		self.get_stop_times()
		rows = self.get_rows()
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
			stops = TRIP_STOPS[TRIP_STOPS['block_id'] == self.train].copy()
			stops.drop_duplicates(subset='stop_id', inplace=True)
			stops = stops[['expected', 'stop_sequence', 'stop_id']]
			stops['expected'] = self.created_at[:DAY_LEN] + stops['expected']
			stops['expected'] = stops['expected'].apply(lambda x: self.format_schedule_time(x))
			return df.merge(stops, left_on='to_id', right_on='stop_id', how='left')
		else:
			df['expected'] = None
			df['stop_sequence'] = None
			return df

	def check_df_valid(self, df):
		if df is None:
			print("not valid none", self.filename)
			return False
		num_stops = len(df)
		valid_len = ((df['from'].nunique() + 1) == num_stops) & (df['to'].nunique() == num_stops)
		if not valid_len:
			print("not valid len", self.filename)
		return valid_len
 
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
	
	def get_train_obj(self, filename):
		train_filename = self.path + filename
		print(train_filename)
		return TrainParser(train_filename)

	def parse_train(self, train):
		"""
		Parse data in a train file to a DataFrame where each row represents a
		pair of stops along journey. 
		
		Creates a TrainParser instance from a train file, parses file data to
		DataFrame, joins schedule data to dataframe, and returns DataFrame.
		"""
		if train.check_file_empty():
			return None
		train_df = train.parse_file_to_df()
		performance = train.join_schedule(train_df)
		if not train.check_df_valid(performance):
			return None
		else:
			return performance[self.df_columns]

	def parse_all_trains(self):
		"""Parse all train files in directory to dataframe (self.all_trains_df).
		If unable to parse train file, store train id (self.invalid_trains).
		"""
		all_trains = []
		for train in self.files:
			train_obj = self.get_train_obj(train)
			train_df = self.parse_train(train_obj)
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

def parse_days(days, path='scraped_trains/'):
	"""Parse all train files for one day into CSV for a day. Write CSV to disk.

	Keyword arguments:
	days -- list of date strings, e.g. ['2018-03-01', '2018-03-02', ...]
	path -- relative folder path where scraped data is stored
	"""
	for date_string in days:
		d = DayParser(path, date_string)
		d.parse_all_trains()
		d.write_day_to_disk()
		print("completed parsing {}".format(date_string))

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


