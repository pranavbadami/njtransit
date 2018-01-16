import requests
import datetime
import time
from bs4 import BeautifulSoup

start = datetime.datetime.now()
resp_count = 1
loop_count = 1
stations = ['NY', 'SE', 'NP', 'NA', 'LI', 'RH', 'MP', 'MU', 'ED', 'NB', 'PJ', 'HL', 'TR']
columns = ['DEP', 'TO', 'TRK', 'LINE', 'TRAIN', 'STATUS']

while True:
	for station in stations:
		try:
			resp = requests.get('http://dv.njtransit.com/mobile/tid-mobile.aspx?sid=' + station, timeout=5)
			if resp.status_code == 200:
				soup = BeautifulSoup(resp.text, "html")
				tables = soup.find_all('table')
				if not len(tables):
					print "no table for", station
				else:
					table_json = 
					for table in tables:
						if table.parent.name == 'td':
							row = 
							for idx,td in enumerate(table.find_all('td')):
								column = columns[idx]

			else:
				pass
		except requests.exceptions.ReadTimeout:
			print "request for", station, "timed out"
		
		resp_count = resp_count + 1
	print "loop count", loop_count
	loop_count = loop_count + 1
	time.sleep(30)
