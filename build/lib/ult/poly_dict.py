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


# called within transforms
# where the last str byte is selected
def lastkey(somedict):
	return dict(map(lambda (key, value): (str(key[-1]), value), somedict.items()))


# function for transforming an entire level of a dictionary
def transform(multilevelDict):
	new = lastkey(multilevelDict)

	for key, value in new.items():
		if isinstance(value, dict):
			new[key] = transform(value)

	return new



# drills a dictionary upwards towards lower level 
# intial keys
# number of levels is how many levels are between
# the new minimum and the previous minimum and need to be created 
def make_layer_dif(dictionary,numberoflevels):
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

def drill_dictionary(multi,lowestlevel):
	level = len(multi.keys()[0])
	multi = make_layer_dif(multi,level - lowestlevel)
	newdict = {}
	for firstkey in multi.keys():
		new = multi[firstkey]
		#new = lastkey(multi[firstkey])
		if isinstance(new,dict):
			new = transform(new)
		newdict[firstkey] = new 
	return newdict


# makes a flat dictionary structure
def make_dict_range(data,areaname,minimumsize,mintotal,**kwargs):
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
def make_drilled_dicts(newlist,mintotal1):
	mintotal = 100
	dictlist = []
	count = 0
	for row in newlist:
		# getting areaname
		areaname = str(np.unique(row['AREA']).tolist()[0])
		
		# getting minimum value
		minval = row['total'].str.len().min()

		# making ind dict
		inddict = make_dict_range(row,areaname,minval,mintotal1)
		

		# appending dict to dict list
		if not inddict == '':
			dictlist.append(inddict)

		# logic for getting the lowest value within all given 
		# tables
		if mintotal > minval:
			mintotal = minval
		print 'Making Each Drilled Dictionary. [%s / %s] [1 / 3]' % (count,len(newlist))
		count += 1
	newdictlist = []
	maxval = 0
	count = 0
	# iterating through each value in dictlist
	for inddict,row in itertools.izip(dictlist,newlist):
		# getting areaname
		areaname = row['AREA'].unique()[0]
		
		# getting minimum value
		minval = row['total'].str.len().min()
		if not minval - mintotal == 0:
			inddict2 = drill_dictionary(inddict,mintotal)
			#inddict2 = inddict
		else:
			inddict2 = inddict
		if row['total'].str.len().max() > maxval:
			maxval = row['total'].str.len().max()
		newdictlist.append(inddict2)
		count+= 1
		print 'Pruning the dicts upwards and clipping keys [%s / %s] [2 / 3]' % (count,len(newlist))
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

def make_dict_attempt(newlist,mintotal):
	drilleddict,minval,maxval = make_drilled_dicts(newlist,mintotal)

	current = minval
	dictionary = {}

	total = drilleddict[0]
	count = 0
	for row in drilleddict[1:]:
		print 'Merging Dictionaries [%s / %s] [3 / 3]' % (count,len(drilleddict))
		total = dict(mergedicts(total,row))
		count += 1
	total['min'] = minval
	total['max'] = maxval
	return total

def make_json(dictionary,filename):
	with open(filename,'wb') as f:
		json.dump(dictionary,f)
	print 'Wrote Json.'

def read_json(filename):
	with open(filename,'rb') as f:
		return json.load(f)



# the map function for the flat dictionary
def find_root_index(thash,indexdict,minsize,areadict):
	try:
		size = minsize
		current = indexdict
		while isinstance(current,dict):
			if size == minsize:
				current = current.get(thash[:size],'')
			else:
				current = current.get(thash[size-1],'')
			size += 1
		return areadict[current]
	except IndexError:
		return ''

# the map function for the flat dictionary
def find_no_area_index(thash,indexdict,minsize):
	try:
		size = minsize
		current = indexdict
		while isinstance(current,dict):
			if size == minsize:
				current = current.get(thash[:size],'')
			else:
				current = current.get(thash[size-1],'')
			size += 1
		return current
	except IndexError:
		return ''


# maps the index dictonary against an area index
def area_index(data,indexdict):
	ind = False
	try:
		areadict = indexdict['areas']
	except KeyError: 
		ind = True
	min = indexdict['min'] 
	areas = []
	ghashs = data['GEOHASH'].values.tolist()
	if ind == False:
		for i in ghashs:
			areas.append(find_root_index(*(i,indexdict,min,areadict)))
	else:
		for i in ghashs:
			areas.append(find_no_area_index(*(i,indexdict,min)))

	data['AREA'] = areas
	return data

# makes a test block and returns the points for the index
def make_test_block(ultindex,number):
	#indexdict = read_json('states_ind.json')
	extrema = {'n':88.9,'s':-30.0,'e':100.0,'w':-180.0}
	data = bl.random_points_extrema(number,extrema)
	data = bl.map_table(data,8,map_only=True)

	s = time.time()
	data = area_index(data,ultindex)
	print 'Time for just indexing: %s' % (time.time() - s)

	data = data[data['AREA'].str.len() > 0]
	data = bl.unique_groupby(data,'AREA')
	data['color'] = data['COLORKEY']
	return data

# lints points for non hashable data types
def lint_values(data):
	for row in data.columns.values.tolist():
		if 'lat' in str(row).lower():
			lathead = row
		elif 'long' in str(row).lower():
			longhead = row
	
	data = data[(data[lathead] < 90.0) & (data[lathead] > -90.0)]
	data = data.fillna(value=0)

	return data[lathead].astype(float).values.tolist(),data[longhead].values.tolist()


# maps the index dictonary against an area index
def area_index2(data,indexdict):
	min = indexdict['min'] 
	areas = []
	
	# getting geohashs
	lats,lngs = lint_values(data)
	ds = []

	for i in range(0,len(lats)):
		oi = (lats[i],lngs[i],12)
 		#newlist.append(oi)
 		ds.append(geohash.encode(*oi))
 		areas.append(find_root_index(*(ds[-1],indexdict,min)))
	data['AREA'] = areas
	data['GEOHASH'] = ds
	return data

def gener(list):
	for row in list:
		yield row

# trading memory overhead for one 
# dictionary method per found area
def construct_area_mask(uniqueareas):
	newdict = {}
	newdict2 = {}
	for i,area in itertools.izip(range(len(uniqueareas)),uniqueareas):
		hexi = str(hex(i))[2:]
		newdict[hexi] = area
		newdict2[area] = hexi
	return newdict,newdict2		  


#returns a list with geojson in the current directory
def get_filetype(src,filetype):
	filetypes=[]
	for dirpath, subdirs, files in os.walk(os.getcwd()+'/'+src):
	    for x in files:
	        if x.endswith('.'+str(filetype)):
	        	filetypes.append(src+'/'+x)
	return filetypes

# wrapper for spark functionality
def set_wrapper(arg):
	data,field,filename,folder = arg
	make_set(data,field,filename,folder=folder,csv=True)
	return []

# constructs a ult-index that can be used in 
# functions like area_index to quickly geofence polygons
def make_set(data,field,filename,**kwargs):
	csv = False
	folder = False
	resume = False
	sc = False
	for key,value in kwargs.iteritems():
		if key == 'csv':
			csv = value
		if key == 'folder':
			folder = value
		if key == 'resume':
			resume = value
		if key == 'sc':
			sc = value

	if not folder == False:
		# creating folder if one doesn't already exist
		if not os.path.exists(folder):
			os.makedirs(folder)

	if csv == True:
		if folder == False:
			prefix = ''
		else:
			# creating folder if one doesn't already exist
			if not os.path.exists(folder):
				os.makedirs(folder)			
			prefix = folder + '/'

	# constructing areas mask
	areamask1,areamask2 = construct_area_mask(np.unique(data[field].astype(str)).tolist())

	# logic for if you want to continue from 
	# what is complete from another directory
	if resume == True:
		files = get_filetype(folder,'csv')
		completedareas = []
		# getting completed areas
		for row in files:
			row = str.split(row,'/')[1]
			row = str.split(row,'.')[0]
			completedareas.append(row)

		# getting unique areas and putting into df
		df = pd.DataFrame(np.unique(data['AREA']).tolist(),columns=['AREA'])
		data['BOOL'] = data['AREA'].astype(str).isin(completedareas)
		noncompletedareas = data[data['BOOL']==False]
		data = noncompletedareas
	
	# logic for handling spark context
	if not sc == False:
		partialargs = np.array_split(data,8)
		newlist = []
		for row in partialargs:
			newlist.append([row,'AREA',filename,folder])
		args = newlist
		instance = sc.parallelize(args)
		instance.map(set_wrapper).collect()

	mintotal = 100
	maxtotal = 0
	totals = []
	
	sizedisplay = len(np.unique(data[field]))
	# grouping each area on the big df
	# so that analysi and indexs for each polygon
	# can be made
	count = 0
	totaltime = 0
	for name,group in data.groupby(field):
		# making ring index for each alignment
		s = time.time()
		total = make_ring_index(group)
		total['AREA'] = areamask2[str(name)]
		#total['AREA'] = areamask2[str(name)]

		# logic for writing csv out to file/folder
		if csv == True:
			csvfilename = prefix + str(name) + '.csv'
			total.to_csv(csvfilename,index=False)

		minval = total['total'].str.len().min()
		maxval = total['total'].str.len().min()

		if minval < mintotal:
			mintotal = minval
		if maxval > maxtotal:
			maxtotal = maxval
		totaltime += time.time() - s
		count += 1

		avg = round(totaltime / float(count),2)
		print 'Areas Complete: [%s / %s], AVGTIME: %s s' % (count,sizedisplay,avg)
		totals.append(total)

	# making the ult index
	ultindex = make_dict_attempt(totals,mintotal)

	# reading json to memory again
	# this is to finally bind the mask 
	ultindex['areas'] = areamask1

	# finally writing out
	make_json(ultindex,filename) 


# collecting geohashs
def collecthash(new,prefix,list):
	for key, value in new.items():
		if not isinstance(new[key],dict):
			list.append([str(prefix+key),str(value)])
		else:
			collecthash(new[key],prefix+key,list)

	return list

# getting all geohashs 
# returns a dataframe of all areas and each geohashs associated with areas
def get_geohashs(b):
	totallist = []
	for row in b.keys():
		prefix = row
		if isinstance(b[row],dict):
			partlist = collecthash(b[row],prefix,[])
			totallist += partlist
	return pd.DataFrame(totallist,columns=['GEOHASH','AREA'])




# reads a list of csv files 
# that contain completly ult indexs 
# for one polygon each
def read_dfs(list):
	mintotal = 100
	maxtotal = 0
	totals = []
	for row in list:
		total = pd.read_csv(row)
		
		# getting max/min geohashs size of current df
		minval = total['total'].str.len().min()
		maxval = total['total'].str.len().min()

		# checking to see if current val is the smallest or largest
		if minval < mintotal:
			mintotal = minval
		if maxval > maxtotal:
			maxtotal = maxval
		
		totals.append(total)
	return totals,mintotal,maxtotal

# function to make a dictonary that is currently 
# in which the ult index is currently csv files 
# in a folder 
def make_progress_dict(folder,filename):
	# getting all appropriate csv files
	files = get_filetype(folder,'csv')

	# reading csv files into memory
	totals,mintotal,maxtotal = read_dfs(files)
	
	# making the ult index
	ultindex = make_dict_attempt(totals,mintotal)

	# writing the large uncleansed index to file
	make_json(ultindex,filename)

	# cleansing file to reduce file size
	# this is the easier of ways to cleanse geohash
	# i.e. this step and following are
	# simply procedures to reduce memory footprint of json
	#make_write(filename,mintotal,maxtotal)

	# reading json to memory again
	# this is to finally bind the mask 
	#ultindex['areas'] = areamask

	# finally writing out
	#make_json(ultindex,filename) 
