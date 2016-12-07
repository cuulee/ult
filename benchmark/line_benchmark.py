import os
import ult
import datetime
import ult
import pandas as pd
import time
import mapkit as mk


def benchmark(current = False):
	data1 = pd.read_csv('test_line.csv')
	# getting old df
	dfold1 = ult.make_line_index(data1,'a.h5',benchmark=True)
	dfold = dfold1
	dfold.columns = ['FIELD','PHILLY_TIMES_OLD']
	print dfold

	if current == False:
		# getting new df
		dfnew1 = make_line_index(data1,'a.h5',benchmark=True)
		dfnew = dfnew1
		dfnew.columns = ['FIELD','PHILLY_TIMES_NEW']
		print dfnew

	time1 = datetime.datetime.now()
	# cleaning up files created during the benchmark
	filename = 'log/line_' + str(time1) + '.csv'


	# combining df
	df = dfold
	df['PHILLY_TIMES_NEW'] = dfnew['PHILLY_TIMES_NEW']
	if current == False:
		df[['FIELD','PHILLY_TIMES_OLD','PHILLY_TIMES_NEW']].set_index('FIELD')
	print df
	df.to_csv(filename)

benchmark(current=True)
