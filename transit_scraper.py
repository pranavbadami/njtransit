from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from rail_data import dv_station_names as dv
import re
import json
import boto3
import os


s3 = boto3.resource('s3')

TERMINALS = {
	"New Bridge Landing":{"abbrev": "NH", "freq":3600},
	"Newark Broad St":{"abbrev": "ND", "freq":1800},
	"Ridgewood":{"abbrev": "RW", "freq":3600},
	"South Amboy":{"abbrev": "CH", "freq":3600},
	"Plainfield":{"abbrev": "PF", "freq":3600},
	"Newark Penn Station":{"abbrev": "NP", "freq":300},
	"Peapack":{"abbrev": "PC", "freq":3600},
	"Murray Hill":{"abbrev": "MH", "freq":3600},
	"Newark Airport":{"abbrev": "NA", "freq":900},
	"Bay Head":{"abbrev": "BH", "freq":3600},
	"Maplewood":{"abbrev": "MW", "freq":3600},
	"Rahway":{"abbrev": "RH", "freq":1800},
	"Bay St (Montclair)":{"abbrev": "MC", "freq":3600},
	"Long Branch":{"abbrev": "LB", "freq":3600},
	"Princeton Junction":{"abbrev": "PJ", "freq":900},
	"Waldwick":{"abbrev": "WK", "freq":3600},
	"Philadelphia":{"abbrev": "PH", "freq":300},
	"Princeton":{"abbrev": "PR", "freq":3600},
	"Montclair State University":{"abbrev": "UV", "freq":3600},
	"High Bridge":{"abbrev": "HG", "freq":3600},
	"Hackettstown":{"abbrev": "HQ", "freq":1800},
	"Lake Hopatcong":{"abbrev": "HP", "freq":3600},
	"Hoboken":{"abbrev": "HB", "freq":1800},
	"Gladstone":{"abbrev": "GL", "freq":3600},
	"Raritan":{"abbrev": "RA", "freq":1800},
	"New Brunswick":{"abbrev":  "NB", "freq":1800},
	"Suffern":{"abbrev": "SF", "freq":3600},
	"Dover":{"abbrev": "DO", "freq":1800},
	"Trenton":{"abbrev": "TR", "freq":300},
	"Metropark":{"abbrev": "MP", "freq":1800},
	"Bernardsville":{"abbrev":  "BV", "freq":3600},
	"Atlantic City Rail Terminal":{"abbrev": "AC", "freq":3600},
	"Spring Valley":{"abbrev":  "SV", "freq":3600},
	"Summit":{"abbrev": "ST", "freq":3600},
	"Mount Olive":{"abbrev":  "OL", "freq":3600},
	"Jersey Ave":{"abbrev": "JA", "freq":3600},
	"New York Penn Station":{"abbrev": "NY", "freq":300},
	"Port Jervis":{"abbrev": "PO", "freq":1800}
}

TRAIN_COLUMN = 4
LINE_COLUMN = 3
DEP_COLUMN = 0
RAIL_DATA = "./rail_data/"
ALL_STATIONS = json.load(open(RAIL_DATA + 'rail_stations'))

trip_stops = pd.DataFrame()
trips = pd.read_csv(RAIL_DATA + 'trips.txt')
stop_times = pd.read_csv(RAIL_DATA + 'stop_times.txt')
trip_stops = stop_times.merge(trips, on=['trip_id'])


class Train:
	url = "http://dv.njtransit.com/mobile/train_stops.aspx?train="
	freq = 60
	statuses = ["DEPARTED", "Cancelled"]
	time_re = re.compile(".*?(\d+):(\d+).*")

	def __init__(self, train_id, line, dep):
		self.id = train_id
		self.line = line
		self.dep = dep
		self.created_at = datetime.now()
		self.scrape_count = 0
		self.data = []
		self.type = self.get_type()
		if self.type == "NJ Transit":
			self.id = train_id.zfill(4) #TODO: format id
			self.buffer_mins = 2
		else:
			self.buffer_mins = 30
		self.scheduled = True
		self.t_scrape = self.get_t_scrape()
		self.completed = False

	def __str__(self):
		return "Train #{}, {}, next scrape: {}".format(self.id, self.line, self.t_scrape)

	def get_type(self):
		try:
			is_int = int(self.id)
			return "NJ Transit"
		except ValueError: 
			return "Amtrak"

	def parse_table(self, soup):
		table = soup.find('table')
		if table is not None:
			return [td.text for td in table.find_all('td')]
		else:
			return []

	def schedule_datetime(self, scheduled):
		hours, minutes, seconds = scheduled.split(":")
		hours, minutes, seconds = int(hours), int(minutes), int(seconds)
		midnight = datetime(year=self.created_at.year,\
							month=self.created_at.month,
							day=self.created_at.day)
		return midnight + timedelta(hours=hours, minutes=minutes)

	def get_scheduled_time(self):
		try:
			scheduled = trip_stops[trip_stops['block_id'] == self.id]['arrival_time'].iloc[0]
			self.scheduled = True
			scheduled = self.schedule_datetime(scheduled) - timedelta(minutes=self.buffer_mins)
			if datetime.now() > scheduled:
				scheduled = datetime.now()

			return scheduled
		except IndexError:
			# train not in schedule
			self.scheduled = False
			return None

	def parse_time(self, hour, minute):
		hour, minute = int(hour), int(minute)
		evening_hour = hour + 12
		if hour >= self.created_at.hour:
			#only possible in morning
			return datetime(year=self.created_at.year, month=self.created_at.month,
								day=self.created_at.day, hour=hour, minute=minute) - timedelta(minutes=self.buffer_mins)
		else:
			if evening_hour >= self.created_at.hour:
				if evening_hour < 24:
					return datetime(year=self.created_at.year, month=self.created_at.month,
							day=self.created_at.day, hour=evening_hour, minute=minute) - timedelta(minutes=self.buffer_mins)
				else:
					evening_hour = evening_hour - 24
					return datetime(year=self.created_at.year, month=self.created_at.month,
							day=self.created_at.day) + timedelta(days=1, hours=evening_hour, minutes=minute) - timedelta(minutes=self.buffer_mins)

			else:
				return datetime(year=self.created_at.year, month=self.created_at.month,
							day=self.created_at.day) + timedelta(days=1, hours=hour, minutes=minute) - timedelta(minutes=self.buffer_mins)

	def approx_dep_time(self, dep):
		dep = dep.replace("\r\n", "")
		match = self.time_re.match(dep)

		if match is not None:
			return self.parse_time(match.group(1), match.group(2))
		else:
			return datetime.now()

	def update_dep(self, dep):
		if not self.scheduled and not self.scrape_count:
			approx_time = self.approx_dep_time(dep)
			if approx_time < self.t_scrape:
				self.t_scrape = approx_time
				self.dep = dep

	def stop_scraping(self):
		latest_data = self.data[-1][1]
		left_system = True

		for stop in latest_data:
			try:
				station, status = stop.split(u"\xa0\xa0")
				if station in ALL_STATIONS:
					left_system = left_system & (("DEPARTED" in status) or ("Cancelled" in status))
			except ValueError:
				pass
		return left_system


	def get_t_scrape(self):
		if self.scrape_count == 0:
			scheduled = self.get_scheduled_time()
			if scheduled is None:
				scheduled = self.approx_dep_time(self.dep)
			return scheduled
		else:
			if self.stop_scraping():
				self.completed = True
				return None
			return self.t_scrape + timedelta(seconds=self.freq)

	def request(self, timeout=3, retry=False):
		try:
			resp = requests.get(self.url + self.id, timeout=timeout)
			if resp.status_code == 200:
				soup = BeautifulSoup(resp.text, "lxml")
				status = self.parse_table(soup)
				return status
			else:
				print "response code {} for {} at {}".format(resp.status_code, self.id, datetime.now())
				return None
		except requests.exceptions.RequestException:
			if retry:
				print "retrying"
				return self.request(timeout=timeout, retry=True)
			else:
				return None

	def scrape(self):
		now = datetime.now()
		data = self.request()
		if data is not None:
			self.scrape_count = self.scrape_count + 1
			self.data.append([now, data])
			self.t_scrape = self.get_t_scrape()

	def write_to_file(self):
		data_dict = {"id": self.id, "line": self.line, 
					 "created_at": self.created_at, "type": self.type, 
					 "scrape_count": self.scrape_count,
					 "scheduled": self.scheduled, "data": self.data}

		date_str = self.created_at.strftime("%Y_%m_%d")
		file_name = '{}_{}'.format(date_str, self.id)
		with open('trains/' + file_name, 'a') as outfile:
			json.dump(data_dict, outfile, default=str)
			outfile.close()

		data = open('trains/' + file_name, 'rb')
		s3.Bucket('njtransit').put_object(Key='/aws/{}/{}'.format(date_str, file_name), Body=data)
		os.remove('trains/' + file_name)


class TerminalScraper:
	terminal_url = "http://dv.njtransit.com/mobile/tid-mobile.aspx?sid="

	def __init__(self):
		self.time = datetime.now()
		self.terminals = TERMINALS
		for term, info in self.terminals.iteritems():
			info['t_scrape'] = self.time

		self.current_trains = {}
		self.completed_trains = {}
		self.time = datetime.now()

	def parse_table(self, soup):
		tables = soup.find_all('table')
		if not len(tables):
			return []
		else:
			trains = []
			for table in tables:
				if table.parent.name == 'td':
					row = table
					cells = row.find_all('td')
					trains.append({'train_id': cells[TRAIN_COLUMN].text, 
								   'line': cells[LINE_COLUMN].text,
								   'dep': cells[DEP_COLUMN].text})
		return trains

	#TODO: change scrape time here
	def get_departures(self, abbrev):
		try:
			resp = requests.get(self.terminal_url + abbrev, timeout=3)
			if resp.status_code == 200:
				soup = BeautifulSoup(resp.text, "lxml")
				return self.parse_table(soup)
			else:
				return []
		except requests.exceptions.ReadTimeout:
			print "request for", abbrev, "timed out"
			return []
		except requests.exceptions.RequestException:
			print "request for", abbrev, "failed (non-timeout)"
			return []



	def get_train_type(self, train_id):
		try:
			is_int = int(train_id) 
			return "NJ Transit"
		except ValueError: 
			return "Amtrak"


	def find_new_trains(self, trains):
		new_trains = []
		for train in trains:
			if "S" not in train['train_id']:
				if not train['train_id'] in self.current_trains:
					if not train['train_id'] in self.completed_trains:
						new_trains.append(train)
				else:
					# update dep time of current train
					self.current_trains[train['train_id']].update_dep(train['dep'])

		return new_trains

	def create_new_trains(self, trains):
		for train in trains:
			if not train['train_id'] in self.current_trains:
				train_obj = Train(train['train_id'], train['line'], train['dep'])
				self.current_trains[train['train_id']] = train_obj
			else:
				self.current_trains[train['train_id']].update_dep(train['dep'])

	def scrape_terminals(self, terminals):
		all_trains = []
		for name in terminals:
			terminal = self.terminals[name]
			trains = self.get_departures(terminal['abbrev'])
			# print terminal, trains
			#TODO: get unique
			self.terminals[name]['t_scrape'] = terminal['t_scrape'] + timedelta(seconds = terminal['freq'])
			all_trains = all_trains + trains
		return all_trains

	def run(self):
		loop_count = 1
		while True:
			now = datetime.now()
			if now.day != self.time.day:
				self.completed_trains = {}

			self.time = now
			#identify terminals to scrape
			scrape_terms = []
			for term, info in self.terminals.iteritems():
				if (info['t_scrape'] <= self.time):
					scrape_terms.append(term)

			all_trains = self.scrape_terminals(scrape_terms)
			new_trains = self.find_new_trains(all_trains)
			self.create_new_trains(new_trains)

			completed = []
			for train_id, train in self.current_trains.iteritems():
				if train.completed:
					print "completed", train_id
					self.completed_trains[train_id] = train
					completed.append(train_id)
					train.write_to_file()
				else:
					if (train.t_scrape <= self.time):
						train.scrape()
			
			for c in completed:
				self.current_trains.pop(c, 0)
			# self.scrape_trains(scrape_trains)
			if not (loop_count % 50):
				print "loop count:", loop_count
			loop_count = loop_count + 1
			time.sleep(10)


def main():
	scraper = TerminalScraper()
	scraper.run()


if __name__ == "__main__":
    main()


