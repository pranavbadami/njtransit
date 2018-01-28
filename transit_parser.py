import json
from rail_data import dv_station_names as dv

ALL_STATIONS = dv.ALL_STATIONS

class TrainParser:

	def __init__(self, filename):
		self.filename = filename
		self.data = self.read(self.filename)


	def read(self, filename):
		try:
			return json.load(open(filename))
		except ValueError:
			print "error reading {}".format(filename)
			return None

	def get_train_meta(self):
		pass

	def get_stop_times(self):
		dep_count = 0
		departures = []
		for frame in self.data['data']:
			time = frame[0]
			stops = frame[1]
			stops = stops[dep_count:]
			for idx, stop in enumerate(stops):
				try:
					station, status = stop.split(u"\xa0\xa0")
					print station, status
				except ValueError:
					station = ""
					status = ""

				if station not in ALL_STATIONS:
					pass
				elif ("DEPARTED" in status):
					print "departed"
					departures.append({'station': station,
									   'time': time,
									   'status': "Departed"})
					dep_count = dep_count + 1
				elif ("Cancelled" in status):
					departures.append({'station': station,
									   'time': time,
									   'status': "Cancelled"})
					dep_count = dep_count + 1
		return departures


