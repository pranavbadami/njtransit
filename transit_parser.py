import json
from rail_data import dv_station_names as dv
import pandas as pd

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

	def get_stop_times(self):
		dep_count = 0
		departures = []
		for frame in self.data['data']:
			time = frame[0]
			stops = frame[1][dep_count:]
			for idx, stop in enumerate(stops):
				try:
					station, status = stop.split(u"\xa0\xa0")
				except ValueError:
					station = ""
					status = ""

				if ("DEPARTED" in status):
					if station in ALL_STATIONS:
						departures.append({'station': station,
										   'time': time,
										   'status': "Departed"})
					dep_count = dep_count + 1
				elif ("Cancelled" in status):
					if station in ALL_STATIONS:
						departures.append({'station': station,
									   'time': time,
									   'status': "Cancelled"})
					dep_count = dep_count + 1

		if len(departures) < len(self.data['data'][0][1]):
			for stop in self.data['data'][0][1][len(departures)+1:]:
				try:
					station, status = stop.split(u"\xa0\xa0")
				except ValueError:
					station = ""
					status = ""
				if station in ALL_STATIONS:
					departures.append({'station': station,
									   'time': time,
									   'status': None})
		return departures

	def get_rows(self, departures):
		prev = departures[0]
		rows = []
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
		df = pd.DataFrame(rows)
		df.set_index("stop_num", inplace=True)
		return df
	
	def join_schedule(self, df):
		if self.scheduled:
			stops = trip_stops[trip_stops['block_id'] == self.train]
			stops = stops[stops['trip_id'] == stops['trip_id'].unique()[0]]
			stops = stops[['expected', 'stop_sequence']]
			stops.set_index('stop_sequence', inplace=True)
			stops['expected'] = self.created_at[:DAY_LEN] + stops['expected']
			return df.join(stops)
		else:
			df['expected'] = None
			df['stop_sequence'] = None
			return df






