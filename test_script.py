import requests
import datetime
import time

start = datetime.datetime.now()
resp_count = 1
loop_count = 1
stations = ['NY', 'SE', 'NP', 'NA', 'LI', 'RH', 'MP', 'MU', 'ED', 'NB', 'PJ', 'HL', 'TR']
while loop_count < 3000:
	for station in stations:
		try:
			resp = requests.get('http://dv.njtransit.com/mobile/tid-mobile.aspx?sid=' + station, timeout=5)
			if resp.status_code == 200:
				print "resp count:", resp_count, datetime.datetime.now(), datetime.datetime.now()-start
			else:
				print "failed, status:", resp.status_code, "count:", count
		except requests.exceptions.ReadTimeout:
			print "request for", station, "timed out"
		
		resp_count = resp_count + 1
	print "loop count", loop_count
	loop_count = loop_count + 1
	time.sleep(30)
