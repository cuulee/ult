import berrl as bl 
import pandas as pd 
import numpy as np 
from polygon_ind import *
import itertools
import time
import os
import json

# makes a flat dictionary structure
def make_dict(data,areaname,**kwargs):
	dictionary = {}
	for key,value in kwargs.iteritems():
		if key == 'dictionary':
			dictionary = value

	# getting max str size
	maxsize = data['total'].str.len().max()
	minsize = maxsize - 1

	# getting dataframes of maximum and minimum size
	mindf = data[data['total'].str.len() == minsize]
	maxdf = data[data['total'].str.len() == maxsize]

	# making all minimum df structures
	for row in mindf.values.tolist():
		dictionary[str(row[0])] = areaname

	# making all maximum df structures
	# getting all unique 4 character maxdf strings
	uniquemaxdfs = np.unique(maxdf['total'].str[:-1]).tolist()	
	for row in uniquemaxdfs:
		tempdict = {}
		oldrow = row
		temp = maxdf[maxdf['total'].str[:-1] == row] 
		for row in temp.values.tolist():
			tempdict[str(row[0])] = areaname
		try:
			values = dictionary[str(oldrow)]
			if isinstance(values,dict):
				values.update(tempdict)
				dictionary[str(oldrow)] = values
		
		except KeyError:
			dictionary[str(oldrow)] = tempdict

	return dictionary


# makes a list of dataframes to a dictionary
def make_dicts(dataframes):
	dictionary = {}
	for row in dataframes:
		dictionary = make_dict(row[0],str(row[1]),dictionary=dictionary)
	return dictionary

# the map function for the flat dictionary
def map_dictionary(thash):
	global indexdict
	global maxsize
	try:
		current = indexdict[thash[:maxsize - 1]]
		if not isinstance(current,dict):
			return current
		else:
			return current[thash[:maxsize]]
	except KeyError:
		return ''

# given a max size returns a list that can be sent into make dicts
def read_sizes(maxsize):
	files = bl.get_filetype('','csv')
	newlist = []
	for row in files:
		if str(maxsize) == row[-5]:
			areaname = row[:-6]
			data = pd.read_csv(row[1:])
			newlist.append([data,areaname[1:]])
	return newlist

# maps the given area to the dictionary given 
# assumes all dicts have the same maxsize currently
def map_area(testdata,dictionary,max):
	global indexdict
	global maxsize 
	maxsize = max
	indexdict = dictionary

	# mapping the data to an area then selecting only the values that
	# fell within an area
	testdata['AREA'] = testdata['GEOHASH'].map(map_dictionary)
	testdata = testdata[testdata['AREA'].str.len() > 0]

	return testdata

# the map function for the flat dictionary
def map_dictionary_mult(thash):
	global indexdict
	global maxsize
	global maxsize 
	global minsize
	try:
		size = minsize
		current = indexdict
		while isinstance(current,dict):
			current = current[thash[:size]]
			size += 1
		return current
	except KeyError:
		return ''


# maps the given area to the dictionary given 
# assumes all dicts have the same maxsize currently
def map_area_mult(testdata,dictionary,min,max):
	global indexdict
	global maxsize 
	global minsize
	global dif
	maxsize = max
	minsize = min
	dif = maxsize - minsize
	indexdict = dictionary

	# mapping the data to an area then selecting only the values that
	# fell within an area
	testdata['AREA'] = testdata['GEOHASH'].map(map_dictionary_mult)
	#testdata = testdata[testdata['AREA'].str.len() > 0]

	return testdata

# given a dataframe containing an alignment returns 
# the maxsize of the precision
def get_maxsize(data):
	indexlist = 0
	size = 3
	maxsize = False
	while indexlist < 250 and maxsize == False:
		size += 1
		innerhashtable = get_inner_hashtable(data,size)
		indexlist = fill_indicies2(data,innerhashtable,size)

	return size

# gets the exact size df it exists and the uniques 
# for the next level
def get_size_uniques(data,size):
	sizeddf = data[data['total'].str.len() == size]
	uniques = np.unique(data['total'].str[:size]).tolist()
	
	#newdf = pd.DataFrame(uniques,columns=['total'])
	return [sizeddf,uniques]

# drills a dictionary upwards towards lower level 
# intial keys
# number of levels is how many levels are between
# the new minimum and the previous minimum and need to be created 
def drill_dictionary(dictionary,numberoflevels):
	count = 0
	while not numberoflevels == count:
		totaldict = {}
		df = pd.DataFrame(dictionary.keys(),columns=['GEOHASH'])
		df['GEOHASH1'] = df['GEOHASH'].str[:-1]
		df = df.groupby('GEOHASH1')
		for name,group in df:
			newdict = {}
			for row in group['GEOHASH'].values.tolist():
				newdict[str(row)] = dictionary[str(row)]
			totaldict[name] = newdict
			#print group
		count += 1
		dictionary = totaldict
	return dictionary

# makes a flat dictionary structure
def make_dict_range(data,areaname,minimumsize,**kwargs):
	dictionary = {}
	for key,value in kwargs.iteritems():
		if key == 'dictionary':
			dictionary = value

	# getting max str size
	maxsize = data['total'].str.len().max()

	# checking dictionary for larger max size
 	#minsize = maxsize - 1

	# getting dataframes of maximum and minimum size
	size = maxsize + 1
	dflist = []
	currenttempdict = {}
	sizes = []
	dicttotal = dictionary
	sizeddf,uniques = get_size_uniques(data,minimumsize)
	if size - minimumsize == 3:
		for row in uniques:
			try:
				values = dicttotal[str(row)]
				if isinstance(values,dict):
					values.update(dicttotal)
					dicttotal[str(row)] = values
			
			except KeyError:
				dicttotal[str(row)] = {}
		for row in sizeddf['total'].values.tolist():
			dicttotal[str(row)] = areaname

		sizeddf,uniques = get_size_uniques(data,minimumsize+1)

		for row in uniques:
			if len(row) == minimumsize + 1:
				if isinstance(dicttotal[row[:-1]],dict):
					dicttotal[row[:-1]][row] = {}
		for row in sizeddf['total'].values.tolist():
			if isinstance(dicttotal[row[:-1]],dict):
				dicttotal[row[:-1]][row] = areaname
		sizeddf,uniques = get_size_uniques(data,minimumsize+2)

		for row in uniques:
			if len(row) == minimumsize + 2:
				if isinstance(dicttotal[row[:-2]],dict):
					#dicttotal[row[:-1]][row] = {}
					try: 	
						dicttotal[row[:-2]][row[:-1]][row] = {}
					except Exception:
						dicttotal[row[:-2]][row[:-1]] = {}

		for row in sizeddf['total'].values.tolist():
				if isinstance(dicttotal[row[:-2]],dict):		
					dicttotal[row[:-2]][row[:-1]][row] = areaname
		return dicttotal

	elif size - minimumsize == 4:
		for row in uniques:
			try:
				values = dicttotal[str(row)]
				if isinstance(values,dict):
					values.update(dicttotal)
					dicttotal[str(row)] = values
			
			except KeyError:
				dicttotal[str(row)] = {}
		for row in sizeddf['total'].values.tolist():
			dicttotal[str(row)] = areaname

		sizeddf,uniques = get_size_uniques(data,minimumsize+1)

		for row in uniques:
			if len(row) == minimumsize + 1:
				if isinstance(dicttotal[row[:-1]],dict):
					dicttotal[row[:-1]][row] = {}
		for row in sizeddf['total'].values.tolist():
				if isinstance(dicttotal[row[:-1]],dict):
					dicttotal[row[:-1]][row] = areaname
		sizeddf,uniques = get_size_uniques(data,minimumsize+2)

		for row in uniques:
			if len(row) == minimumsize + 2:
				if isinstance(dicttotal[row[:-2]],dict):
					#dicttotal[row[:-1]][row] = {}
					try:
						dicttotal[row[:-2]][row[:-1]][row] = {}
					except Exception:
						dicttotal[row[:-2]][row[:-1]] = {}

		for row in sizeddf['total'].values.tolist():
				if isinstance(dicttotal[row[:-2]],dict):		
					dicttotal[row[:-2]][row[:-1]][row] = areaname

		sizeddf,uniques = get_size_uniques(data,minimumsize+3)

		for row in uniques:
			if len(row) == minimumsize + 3:
				if isinstance(dicttotal[row[:-3]],dict):
					#dicttotal[row[:-1]][row] = {}
					try:
						dicttotal[row[:-3]][row[:-2]][row[:-1]][row] = {}
					except Exception:
						dicttotal[row[:-3]][row[:-2]][row[:-1]] = {}
		
		for row in sizeddf['total'].values.tolist():
				if isinstance(dicttotal[row[:-3]],dict):		
					dicttotal[row[:-3]][row[:-2]][row[:-1]][row] = areaname

		return dicttotal
	elif size - minimumsize == 5:
		for row in uniques:
			try:
				values = dicttotal[str(row)]
				if isinstance(values,dict):
					values.update(dicttotal)
					dicttotal[str(row)] = values
			
			except KeyError:
				dicttotal[str(row)] = {}
		for row in sizeddf['total'].values.tolist():
			dicttotal[str(row)] = areaname

		sizeddf,uniques = get_size_uniques(data,minimumsize+1)

		for row in uniques:
			if len(row) == minimumsize + 1:
				if isinstance(dicttotal[row[:-1]],dict):
					dicttotal[row[:-1]][row] = {}
		for row in sizeddf['total'].values.tolist():
			if isinstance(dicttotal[row[:-1]],dict):
				dicttotal[row[:-1]][row] = areaname
		sizeddf,uniques = get_size_uniques(data,minimumsize+2)

		for row in uniques:
			if len(row) == minimumsize + 2:
				if isinstance(dicttotal[row[:-2]],dict):
					#dicttotal[row[:-1]][row] = {}
					#print isinstance(dicttotal[row[:-2]][row[:-1]],dict)
					try:
						dicttotal[row[:-2]][row[:-1]][row] = {}
					except TypeError:
						dicttotal[row[:-2]][row[:-1]] = {}
		for row in sizeddf['total'].values.tolist():
				if isinstance(dicttotal[row[:-2]],dict):		
					try:
						dicttotal[row[:-2]][row[:-1]][row] = areaname
					except TypeError:
						dicttotal[row[:-2]][row[:-1]] = areaname

		sizeddf,uniques = get_size_uniques(data,minimumsize+3)

		for row in uniques:
			if len(row) == minimumsize + 3:
				if isinstance(dicttotal[row[:-3]],dict):
					#dicttotal[row[:-1]][row] = {}
					try:
						dicttotal[row[:-3]][row[:-2]][row[:-1]][row] = {}
					except Exception:
						dicttotal[row[:-3]][row[:-2]][row[:-1]] = {}

		for row in sizeddf['total'].values.tolist():
				if isinstance(dicttotal[row[:-3]],dict):		
					dicttotal[row[:-3]][row[:-2]][row[:-1]][row] = areaname

		sizeddf,uniques = get_size_uniques(data,minimumsize+4)

		for row in uniques:
			if len(row) == minimumsize + 4:
				if isinstance(dicttotal[row[:-4]],dict):
					#dicttotal[row[:-1]][row] = {}
					try:
						dicttotal[row[:-4]][row[:-3]][row[:-2]][row[:-1]][row] = {}
					except:
						dicttotal[row[:-4]][row[:-3]][row[:-2]][row[:-1]] = {}

		for row in sizeddf['total'].values.tolist():
				if isinstance(dicttotal[row[:-4]],dict):		
					dicttotal[row[:-4]][row[:-3]][row[:-2]][row[:-1]][row] = areaname

		return dicttotal
	elif size - minimumsize == 2:
		for row in uniques:
			try:
				values = dicttotal[str(row)]
				if isinstance(values,dict):
					values.update(dicttotal)
					dicttotal[str(row)] = values
			
			except KeyError:
				dicttotal[str(row)] = {}

		for row in sizeddf['total'].values.tolist():
			dicttotal[str(row)] = areaname

		sizeddf,uniques = get_size_uniques(data,minimumsize+1)

		for row in uniques:
			if len(row) == minimumsize + 1:
				if isinstance(dicttotal[row[:-1]],dict):
					dicttotal[row[:-1]][row] = {}
		for row in sizeddf['total'].values.tolist():
			if isinstance(dicttotal[row[:-1]],dict):
				dicttotal[row[:-1]][row] = areaname		
		return dicttotal
	elif size - minimumsize == 1:
		for row in uniques:
			try:
				values = dicttotal[str(row)]
				if isinstance(values,dict):
					values.update(dicttotal)
					dicttotal[str(row)] = values
			
			except KeyError:
				dicttotal[str(row)] = {}

		for row in sizeddf['total'].values.tolist():
			dicttotal[str(row)] = areaname

		return dicttotal
	return ''

# given a set of upper precisions selects the csv files
# in the directory with the lower and upper for each
# and returns a dictionary object 
def make_rangedicts(maxlist):
	total = []
	minimumsize = 1000
	for row in maxlist:
		datalist1 = read_sizes(row)
		total += datalist1
		minval = row - 1
		if minval < minimumsize:
			minimumsize = minval
	dictionary = {}
	for row in total:
		dictionary = make_dict_range(row[0],row[1],minimumsize,dictionary=dictionary)

	return dictionary

# makes a list of each df table given
# then after finding the minimum total value
# drills each dict up to the highest (lowest len) value
def make_drilled_dicts(newlist):
	mintotal = 100
	dictlist = []
	count = 0
	for row in newlist:
		# getting areaname
		areaname = str(np.unique(row['area']).tolist()[0])
		
		# getting minimum value
		minval = row['total'].str.len().min()

		# making ind dict
		inddict = make_dict_range(row,areaname,minval)

		# appending dict to dict list
		if not inddict == '':
			dictlist.append(inddict)

		# logic for getting the lowest value within all given 
		# tables
		if mintotal > minval:
			mintotal = minval
		print count
		count += 1
	newdictlist = []
	maxval = 0
	count = 0
	# iterating through each value in dictlist
	for inddict,row in itertools.izip(dictlist,newlist):
		# getting areaname
		areaname = row['area'].unique()[0]
		
		# getting minimum value
		minval = row['total'].str.len().min()
		if not minval - mintotal == 0:
			inddict2 = drill_dictionary(inddict,minval - mintotal)
		else:
			inddict2 = inddict
		if row['total'].str.len().max() > maxval:
			maxval = row['total'].str.len().max()
		newdictlist.append(inddict2)
		print count
		count+= 1
	return newdictlist,mintotal,maxval

def mergedicts(dict1, dict2):
    for k in set(dict1.keys()).union(dict2.keys()):
        if k in dict1 and k in dict2:
            if isinstance(dict1[k], dict) and isinstance(dict2[k], dict):
                yield (k, dict(mergedicts(dict1[k], dict2[k])))
            else:
                # If one of the values is not a dict, you can't continue merging it.
                # Value from second dict overrides one in first and we move on.
                yield (k, dict2[k])
                # Alternatively, replace this with exception raiser to alert you of value conflicts
        elif k in dict1:
            yield (k, dict1[k])
        else:
            yield (k, dict2[k])

def merge(a, b, path=None):
    "merges b into a"
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            else:
            	pass
        else:
            a[key] = b[key]
    return a

def make_dict_attempt(newlist):
	drilleddict,minval,maxval = make_drilled_dicts(newlist)

	current = minval
	dictionary = {}

	total = drilleddict[0]
	count = 0
	for row in drilleddict[1:]:
		print count
		total = dict(mergedicts(total,row))
		count += 1
	return total

def make_json(dictionary,filename):
	with open(filename,'wb') as f:
		json.dump(dictionary,f)
	print 'Wrote Json.'
def read_json(filename):
	with open(filename,'rb') as f:
		return json.load(f)


'''
bl.clean_current()
data = pd.read_csv('aa.csv')
data1 = pd.read_csv('a.csv')
extrema = get_extrema(data1)
#extrema = {'n':41.0,'s':39.0,'e':-75.0,'w':-80.0}
testdata = bl.random_points_extrema(10000,extrema)
testdata = bl.map_table(testdata,8,map_only=True)

dictarea = make_dict_range(data,'aa',6)

s = time.time()
testdata = map_area_mult(testdata,dictarea,6,8)
print time.time() - s

c1 = bl.get_heatmap51()[0]
c51 = bl.get_heatmap51()[-1]

df1 = testdata[testdata['AREA'].str.len() > 0]
df2 = testdata[testdata['AREA'].str.len() == 0]
df1['COLORKEY'] = c51
df2['COLORKEY'] = c1

testdata = pd.concat([df1,df2])
print testdata
bl.make_points(testdata,filename='p.geojson')
blocks = bl.make_geohash_blocks(data['total'].values.tolist())
blocks['COLORKEY'] = c51
bl.make_blocks(blocks,filename='b.geojson')
bl.a(colorkey='COLORKEY')
'''