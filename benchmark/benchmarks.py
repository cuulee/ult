import os
import time
import pandas as pd
from ult import make_set,make_h5_output,readh,area_index
import datetime
import sys


# a set of rough benchmarks for each output type / io process
def bench_make_polygon_index(var,type):
	if type == 'single':
		filename = 'single.h5'
		readfile = 'test_single.csv'
	elif type == 'regions':
		filename = 'regions.h5'
		readfile = 'test_regions.csv'
	data = pd.read_csv(readfile)
	count = 0
	newlist = []
	while not count == var:
		try:
			os.remove('progress.h5')
		except:
			pass		
		s = time.time()
		make_set(data,'AREA',output=type,printoff=True)
		e1 = time.time() - s
		s1 = time.time()
		make_h5_output(filename,output=type,printoff=True)
		e2 = time.time() - s1
		etotal = time.time() - s
		newlist.append([e1,e2,etotal])
		count += 1
		print 'Total time %s in %s single iteration.' % (etotal,count)
	return pd.DataFrame(newlist,columns=['make_set("%s.h5")' % type,'make_h5_output(("%s.h5")' % type,'total("%s.h5")' % type]).mean()

def bench_read(var,type):
	if type == 'single':
		filename = 'single.h5'
		readfile = 'test_single.csv'
	elif type == 'regions':
		filename = 'regions.h5'
		readfile = 'test_regions.csv'
	count = 0
	newlist = []
	while not count == var:
		s = time.time()
		readh(filename)
		e = time.time() - s
		count += 1
		newlist.append(e)
	return pd.DataFrame(newlist,columns=['readh("%s.h5")' % type]).mean()

def bench_output(var,type):
	if type == 'single':
		filename = 'single.h5'
		readfile = 'test_single.csv'
	elif type == 'regions':
		filename = 'regions.h5'
		readfile = 'test_regions.csv'
	from pipegeohash import random_points_extrema,map_table
	count = 0
	newlist = []
	index = readh(filename)

	while not count == var:
		extrema = {'n': 80.449779,'s': 10.565774,'e':-60.493017,'w':-150.578836}
		data = random_points_extrema(1000000,extrema)
		data = map_table(data,12,map_only=True)
		s = time.time()
		area_index(data,index)
		e = time.time() - s
		newlist.append(e)
		count += 1
	return pd.DataFrame(newlist,columns=['1M INDEXS("%s.h5")' % type]).mean()


def make_bench(var):
	count = 0
	makesingle = bench_make_polygon_index(var,'single')
	print 'Completed the creation benchmark single.'
	readsingle = bench_read(var,'single')
	print 'Completed the read benchmark single.'
	outputsingle = bench_output(var,'single')
	print 'Completed the output benchmark single.'


	df1 = pd.concat([makesingle,readsingle,outputsingle]).reset_index()
	
	makereg = bench_make_polygon_index(var,'regions')
	print 'Completed the creation benchmark regions.'
	readreg = bench_read(var,'regions')
	print 'Completed the read benchmark regions.'
	outputreg = bench_output(var,'regions')
	print 'Completed the output benchmark regions.'


	df2 = pd.concat([makereg,readreg,outputreg]).reset_index()

	df1.columns =['single','single_values']
	df1 = df1[['single','single_values']]

	df2.columns =['regions','regions_values']
	df2 = df2[['regions','regions_values']]
	df = pd.concat([df1,df2],axis=1)

	time1 = datetime.datetime.now()

	# cleaning up files created during the benchmark
	filename = 'log/' + str(time1) + '.csv'
	df.to_csv(filename)
	filenames = ['progress.h5','single.h5','regions.h5','filename']
	for i in filenames:
		try:
			os.remove(i)
		except:
			pass
	print pd.read_csv(filename),'\nResults of Benchmark!'

def main():
	# print command line arguments
	for arg in sys.argv[1:]:
		var = int(arg)
	if not type(var) == int:
		var = 1
	print 'Performing %s benchmark iterations' % var
	make_bench(var)
main()


