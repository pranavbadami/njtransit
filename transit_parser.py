import json
from rail_data import dv_station_names as dv

ALL_STATIONS = dv.ALL_STATIONS

class TrainParser:

	def __init__(self, filename):
		self.filename = filename
		self.data = self.read(self.filename)
		self.train, self.line, self.type = self.get_meta()

	def read(self, filename):
		try:
			return json.load(open(filename))
		except ValueError:
			print "error reading {}".format(filename)
			return None

	def get_meta(self):
		return self.data['id'], self.data['line'], self.data['type']

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

	def format_as_csv(self, departures):
		prev = departures[0]

		for departure in departures:
			row = [self.train, self.line, self.type]
			row = row + [prev['station'], departure['station'], departure['time'], departure['status']]
			prev = departure
			print row




