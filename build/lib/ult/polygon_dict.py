import pandas as pd 
import numpy as np 
import itertools
import time
import os
import simplejson as json
from pipegeohash import map_table,random_points_extrema,lint_values
from polygon_ind import make_ring_index
import geohash
import kyoto as kt


# sringify the output of a line segment
def stringify(coords):
	newlist = []
	for long,lat in coords:
		newlist.append('[%s, %s]' % (long,lat))
	return '[' + ', '.join(newlist) + ']'

# given a dataframe and minimum size 
# returns the uniques within the dataframe
# that exist in between the minsizie and lowest
# level values
def reduce_to_min(data,**kwargs):
	data = (data.to_dense()
			.fillna(method='ffill')
			.reset_index())
	maxsize = data['total'].str.len().max()
	minsize = data['total'].str.len().min()
	for key,value in kwargs.iteritems():
		if key == 'minsize':
			minsize = value
	current = maxsize - 1
	uniques = []
	count = 0
	while not current == minsize - 1:
		current = current - 1

		tempuniques = np.unique(data['total'].str[:current+1]).tolist()
		uniques = np.unique(uniques + tempuniques).tolist()	
	return uniques,maxsize,minsize


def merge_dicts(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

# opens a file and parse lines
def openfile(filename):
	with open(filename,'rb') as f:
		return str.split(f.read(),'\n')

# combines csv files to a big csv much much faster
def combine_csvs(files,outfilename,**kwargs):
	min = False
	for key,value in kwargs.iteritems():
		if key == 'min':
			min = value


	count = 0
	totalcount = 0
	for filename in files:
		data = openfile(filename)

		if count == 0:
			count = 1
			total = data
		else:
			count+= 1
			total += data[1:]
		if count == 1000:
			count = 1
			totalcount += 1000
			print '[%s/%s]' % (totalcount,len(files))
	total = '\n'.join(total)

	with open(outfilename,'wb') as f:
		f.write(total)


# makes a test block and returns the points for the index
def make_test_block(ultindex,number):
	from mapkit import unique_groupby
	from pipegeohash import map_table,random_points_extrema
	#indexdict = read_json('states_ind.json')
	extrema = {'n': 50.449779,'s': 20.565774,'e':-64.493017,'w':-130.578836}
	data = random_points_extrema(number,extrema)
	print data
	data = map_table(data,12,map_only=True)
	print data
	s = time.time()
	data = area_index(data,ultindex)
	print 'Time for just indexing: %s' % (time.time() - s)
	data = data[data['AREA'].str.len() > 0]
	data = unique_groupby(data,'AREA',hashfield=True,small=True)
	data['color'] = data['COLORKEY']
	return data


# maps one geohash to what it needs to be
# as simple as it gets
def one_polygon_index(ghash):
	global minsize
	global maxsize
	global ultindex
	global areamask
	current = minsize
	while current < maxsize:
		output = ultindex.get(ghash[:current],'')
		if output == -1:
			current += 1
		elif output == '':
			return ''
		else:
			return areamask.get(output,'')

# maps one geohash to what it needs to be
def one_polygon_index_regions(ghash):
	global minsize
	global maxsize
	global ultindex
	global areamask
	global current 
	currentindex = ultindex.get(ghash[:2],{})
	if currentindex == {}:
		return ''
	current = minsize
	while current < maxsize:
		output = currentindex.get(ghash[:current],'')
		#print output,ghash,current,ghash[:current]
		# logic for continuing or not
		if output == -1:
			current += 1
		elif output == '':
			return ''
		else:
			return areamask.get(output,'')

# maps a function ane encapsolates a groupby
def mapfunc(data):
	global totalultindex
	global ultindex
	global count

	name = data.name	
	data = data.set_index('i')
 	name = str(data['wam'][:1].values.tolist()[0])
 	try:
		ultindex = totalultindex[name]
		print '[%s / %s] Iterating through each region. ' % (count,len(totalultindex.keys()))
		count += 1
	except:
		data['a'] = ''
		return data['a']
	return data['GEOHASH'].map(one_polygon_index)


# maps a geohash to a given area index
# will again use global dict instead of function input
# this is the main function you should use to generate outputs
# data is a dataset containing a geohash field
# index is the top level index data structure
# objects within the h5 file handle the difference between 
# regions and single output files
def area_index(data,index):
	global minsize
	global maxsize
	global ultindex
	global areamask
	ultindex = index['ultindex']
	areamask = index['areamask']
	minsize = index['metadata']['minsize']
	maxsize = index['metadata']['maxsize']
	output_type = index['metadata']['output_type']
	if output_type == 'single':
		data['AREA'] = data['GEOHASH'].map(one_polygon_index)
		return data
	elif output_type == 'regions':
		global totalultindex 
		global count 
		count = 0
		data['i'] = range(len(data))
		totalultindex = ultindex
		data['wam'] = data['GEOHASH'].str[:2]
		
		# getting the output 
		holder = data.groupby('wam').apply(mapfunc) #.set_index('GEOHASH')[0]
		
		# selecting only the good value sfrom the output and rewinding
		# the index back up
		dataholder = holder.reset_index()
		dataholder = dataholder[dataholder[0].astype(str) != ''] 
		dataholder = dataholder.set_index(['wam','i'])
		data = data.set_index(['wam','i'])
		data = data.loc[dataholder.index]
		data['AREA']  = dataholder[0]
		data = data.reset_index()
		return data

# simple iterattor
def gener(list):
	for row in list:
		yield row

# trading memory overhead for one 
# dictionary method per found area
def construct_area_mask(uniqueareas):
	newdict = {}
	newdict2 = {}
	newdict = dict.fromkeys(range(len(uniqueareas)))
	for i,area in itertools.izip(range(len(uniqueareas)),uniqueareas):
		hexi = i
		newdict[int(i)] = area
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

# creates a bounds dataframe taht can be used to display
# boundries, input is the total boundry df
def stringify_bounds(data):
	newlist = []
	for name,group in data.groupby(['AREA','PART']):
		group = group.reset_index()
		coords = group[['LONG','LAT']].values.tolist()
		coords = stringify(coords)
		newlist.append([name[0],name[1],coords])
	newlist = pd.DataFrame(newlist,columns=['AREA','PART','coords'])
	return newlist

# creates metadata dictionary for polygon h5 outputs
# type is the output type
# min and max is the size of polygon steps
# size is the size of areamask
def make_meta_polygon(type,min,max,size):
	return """{"type":"polygons","output_type":"%s","minsize":%s,"maxsize":%s,"size":%s}""" % (type,min,max,size)


# getting the files in a regions output 
def make_files(dflist):
	files = []
	with pd.HDFStore('progress.h5') as progress:
		for alist in dflist:
			print alist
			files.append(progress[str(alist)])			
	return files


# makes a json string representation given a df or dictionary object
def make_json_string(data,intbool=False):
	if isinstance(data,dict):
		data = pd.DataFrame(data.items(),columns=['GEOHASH','AREA'])

	# creating output string for dataframe
	if intbool == True:
		data['STRING'] = '"' + data['GEOHASH'].astype(str) + '":' + data['AREA'].astype(str) 	
	else:
		data['STRING'] = '"' + data['GEOHASH'].astype(str) + '":"' + data['AREA'].astype(str) + '"'
	
	data['a'] = 'z'
	return '{%s}' % data.groupby('a')['STRING'].apply(lambda x:"%s" % ', '.join(x))['z']


# this function creates an hdf5 output that contains
# an ultindex dictionary the actual output
# assumes input will be progress h5 in the current directroy
# kwargs will control how the output is handle mutliple dict etc.
def make_h5_output(filename,**kwargs):
	output = 'single'
	printoff = False
	for key,value in kwargs.iteritems():
		if key == 'output':
			output = value
		if key == 'printoff':
			printoff = value
	starth5 = time.time()

	# reading in progress h5 file 
	with pd.HDFStore('progress.h5') as progress:
		completed = progress.keys()
		newcompleted = []

		# getting alignment df input into make_set
		alignment = progress['/initial']
		areamask,m2 = construct_area_mask(np.unique(alignment['AREA'].astype(str)).tolist())


		# getting all completed dfs
		for row in completed:
			if not row == '/initial' and not row == '/areamask':
				newcompleted.append(row)
		
		dflist = []
		for row in newcompleted:
			if output == 'single':
				dflist.append(progress[str(row)])
			else:
				dflist.append(str(row))
	
	# logic for a single output dictionary for either progress
	# or completion should be used with smaller indexs
	if output == 'single':
		# stringify bounds
		bounds = stringify_bounds(alignment)

		# concatenating all the dataframes
		dflist = pd.concat(dflist,ignore_index=True).to_dense().fillna(method='ffill')

		# getting the min and maximum
		minsize = dflist['total'].str.len().min()
		maxsize = dflist['total'].str.len().max()

		# creating the drilled dataframe 
		uniques,b,c = reduce_to_min(dflist,minsize=minsize)
		total2 = pd.DataFrame(np.unique(uniques).tolist(),columns=['total'])
		total2['AREA'] = -1


		# setting up dictionaries (combing the finished dataframes)
		dflist = dflist.set_index('total')
		total2 = total2.set_index('total')
		dflist = dflist['AREA'].to_dict()
		total2 = total2['AREA'].to_dict()

		# making total dict
		totaldict = merge_dicts(*[total2,dflist])

		# making the json string this will be placed into a datarame
		# then read into memory upon reading in a speific read_fucntion for ultindex
		jsonstring = make_json_string(totaldict,intbool=True)

		# making meta dictioanry		
		areamask = json.dumps(areamask)
		metadict = make_meta_polygon(output,minsize,maxsize,len(areamask))
		data = pd.DataFrame([jsonstring,metadict,areamask],index=['ultindex','metadata','areamask'])

		with pd.HDFStore(filename) as out:
			out['combined'] = data
			out['alignmentdf'] = bounds

	if output == 'regions':
		# stringify bounds
		bounds = stringify_bounds(alignment)

		dflist = make_files(dflist)
		newdict = {}
		mins = []
		maxs = []
		# this block of code iterates through all dfs in a 
		# the  progress dict and groups them by 
		# there first two geohashs 
		# the extent regionsare currentl aggegrated
		for row in dflist:
			minsize = row['total'].str.len().min()
			maxsize = row['total'].str.len().max()
			mins.append(minsize)
			maxs.append(maxsize)
			row = row.reset_index()
			row['G1'] = row['total'].str[:2]
			for name,group in row.groupby('G1'):
				try:
					newdict[name].append(group)
				except KeyError:
					newdict[name] = [group]

		size = len(newdict.keys())
		count = 0
		ultindex = {}
		minsize = min(mins)
		maxsize = max(maxs)
		for i in newdict.keys():
			count += 1
			df = pd.concat(newdict[i])
			df = df[['total','AREA']]
			uniques,b,c = reduce_to_min(df,minsize=minsize)
			df1 = pd.DataFrame(uniques,columns=['total'])
			df.columns = ['GEOHASH','AREA']
			df1.columns = ['GEOHASH']

			df1['AREA'] = np.nan
			df1['AREA'].iloc[0] = -1
			df = pd.concat([df1.to_sparse(),df])


			# turining into sparse dataframe
			
			ultindex[str(i)] = make_json_string(df.to_dense().fillna(method='ffill'),intbool=True)

			#df.to_csv(str(i)+'.csv',index=False)
			print '[%s / %s]' % (count,size)
		
		# making meta dictioanry 		
		areamask = json.dumps(areamask)

		# creating the metadata dictonary
		metadata = make_meta_polygon(output,minsize,maxsize,len(areamask))
		
		data1 = pd.DataFrame(ultindex.items(),index=ultindex.keys())
		data2 = pd.DataFrame([metadata,areamask],index=['metadata','areamask'])
		# combining the two dfs that contain embedded sudo dictionary objects
		# index:string  of dictionary is contained in every value in this df
		data = pd.concat([data1[1],data2])


		# writing out two top level dataframes to the output h5 file
		with pd.HDFStore(filename) as out:
			out['combined'] = data
			out['alignmentdf'] = bounds


# this is the spark worker process in which 
# spark arguments are parrelized against. 
# in other words this gets an even amount
# of area inputs from each process
def make_wrapper(args):
	data,areamask2,process = args
	totals = []
	count = 0
	totalcount = 0
	current = 0
	make_json({'status':False},'ind.json')
	size = len(np.unique(data['AREA']))
	for name,group in data.groupby('AREA'):
		# making ring index for each alignment
		s = time.time()
		total = make_ring_index(group)

		# adding the area field into thesparse dataframe
		total['AREA'] = np.nan
		total['AREA'].iloc[0] = areamask2[str(name)]

		# getting the min & max values
		minval = total['total'].str.len().min()
		maxval = total['total'].str.len().min()

		# appending solved dataframe to the total
		totals.append(total)
		totalcount += 1
		current += 1

		# this logic controls the write context to the progress h5 file
		# without this json lock two write would happen at once and kill the file
		if current == 50 or totalcount >= size:
			current = 0
			totals = pd.concat(totals).to_dense()
			ind = 0
			while ind == 0:
				time.sleep(1)
				if read_json('ind.json')['status'] == False:
					make_json({'status':True},'ind.json')
					with pd.HDFStore('progress.h5') as progress:
						progress[str(name)] = totals
					ind = 1
			make_json({'status':False},'ind.json')
			totals = []
		print '[%s/%s] for Process:%s in %s' % (totalcount,size,process,time.time() - s)		
	return []


# constructs a ult-index that can be used in 
# functions like area_index to quickly geofence polygons
def make_set(data,field,**kwargs):
	makesetstart = time.time()
	sc = False
	printoff = False
	for key,value in kwargs.iteritems():
		if key == 'sc':
			sc = value
		elif key == 'printoff':
			printoff = value

	# logic for getting only the areas that we havent already computed
	# (checks the progress h5 file for collisions)
	# constructing areas mask
	areamask1,areamask2 = construct_area_mask(np.unique(data[field].astype(str)).tolist())
	sizeareas = len(np.unique(data[field])) + 2
	try:
		with pd.HDFStore('progress.h5') as progress:
			try:
				progress['initial'] = data
			except:
				progress = pd.HDFStore('progress.h5')
				progress['initial'] = data
				
			completed = progress.keys()
			startcount = len(completed)
			completed = [row[1:] for row in completed]
			data['BOOL'] = data[field].isin(completed)
			data = data[data['BOOL'] == False]
	except:
		progress = pd.HDFStore('progress.h5')
		progress.close()
		startcount = 0

	# logic for handleling spark context
	# if a sparck context (sc) is passed in as a kwarg
	# will deviate entire rest of function to a parrelizeed
	# worker processes splitting up the areas remaining
 	if not sc == False:
		#splits = np.array_split(data['COMB'].values,8)
		areas = np.unique(data['AREA'])
		data = data.set_index('AREA')
		count = 0
		args = []
		for i in np.array_split(areas,8):
			split = data.loc[i].reset_index()
			args.append([split,areamask2,count])
			count += 1
		

		instance = sc.parallelize(args)
		instance.map(make_wrapper).collect()
		return []

	mintotal = 100
	maxtotal = 0
	totals = []
	sizedisplay = len(np.unique(data[field]))
	# grouping each area on the big df
	# so that analysi and indexs for each polygon
	# can be made
	count = startcount
	totaltime = 0
	count2 = 0
	totalslist = []
	counterwrite = 0		
	size = len(np.unique(data['AREA']))
	# this is the main line loop of the function
	# its iterating thorugh each area against the input dataframe
	for name,group in data.groupby(field):
		# making ring index for each alignment
		s = time.time()
		total = make_ring_index(group,printoff=printoff)


		# adding area with np.nan as dominat value
		total['AREA'] = np.nan
		total['AREA'].iloc[0] = areamask2[str(name)]

		#total['AREA'] = areamask2[str(name)]


		minval = total['total'].str.len().min()
		maxval = total['total'].str.len().min()

		if minval < mintotal:
			mintotal = minval
		if maxval > maxtotal:
			maxtotal = maxval
		totaltime += time.time() - s
		count += 1
		avg = round(totaltime / float(count - startcount),2)
		if printoff == False:
			print 'Areas Complete: [%s / %s], AVGTIME: %s s' % (count,sizeareas,avg)
		
		counterwrite += 1
		totalslist.append(total)
		if counterwrite == 20 or count >= size:
			counterwrite = 0
			totalslist = pd.concat(totalslist).to_sparse()
			with pd.HDFStore('progress.h5') as progress:
				progress[str(name)] = totalslist
			totalslist = []

	avg = round(time.time() - makesetstart,4)


	return areamask1,mintotal,maxtotal


# makes a polygon index from a dataframe containing typical 
# polygon layout
# data - is the dataframe
# filename - is output h5 file that will be made
# retain progress.h5 file (normally deleted after h5 output created)
def make_polygon_index(data,filename,**kwargs):
	output = 'single'
	sc = False
	retain_progress = False
	for key,value in kwargs.iteritems():
		if key == 'output':
			output = value
		if key == 'sc':
			sc = value
		if key == 'retain_progress':
			retain_progress =value

	# making ech indiviual dataframee solve for each area
	make_set(data,'AREA',sc=sc)
	print 'Made progress h5 file now constructing ouput file.'

	# taking those subsequent dataframes and creating the output
	make_h5_output(filename,output=output)
	print 'Made output h5 file containing datastructures:'
	print '\t- alignmentdf (type: pd.DataFrame)'
	print '\t- areamask (type: dict)'
	print '\t- ultindex (type: dict)'
	print '\t- metadata (type: dict)'
	

	if retain_progress == False:
		os.remove('progress.h5')
		print 'Removed progress.h5 from directory'


# creates a blocks dataframe from the ultindex
def make_blocks_polygons(index):
	# getting areamask
	areamask = index['areamask']
	
	# creating dataframe
	data = pd.DataFrame(index['ultindex'].items(),columns=['GEOHASH','AREA'])
	
	# filtering out upper hiearcharchy
	data = data[data.AREA != -1]
	
	# applying area mask to get the areas
	data['AREA'] = data['AREA'].map(lambda x:areamask[x])
	return data


# creating the lines 
def make_blocks_lines(index):
	areamask = index['areamask']
	data = index['alignmentdf']
	return data

# creates a configuration for all types
def make_all_types_polygons(pointdata,index):
	from mapkit import make_config
	# getting the area,colorkey dict
	colordict = pointdata.set_index('AREA')['COLORKEY'].to_dict()

	# getting lines and blocks dataframes
	lines = make_blocks_lines(index)
	blocks = make_blocks_polygons(index)

	# filtering lines and blocks
	lines['BOOL'] = lines['AREA'].isin(colordict.keys())
	blocks['BOOL'] = blocks['AREA'].isin(colordict.keys())
	lines = lines[lines.BOOL == True]
	blocks = blocks[blocks.BOOL == True]

	# adding colorkey to lines and blocks
	lines['COLORKEY'] = lines['AREA'].map(lambda x:colordict[x])
	blocks['COLORKEY'] = blocks['AREA'].map(lambda x:colordict[x])

	a = make_config(pointdata,'points')
	a = make_config(lines,'lines',current=a)
	a = make_config(blocks,'blocks',current=a)
	return a

# given a point and line frame
def make_both(points,lines):
	from mapkit import make_config
	return make_config(points,'points',current=make_config(lines,'lines'))

# functon for expanding out a list of sparse dfs to a dictionary
# this processes is performed lazily
def make_sparse_big(index):
	keys = index['ultindex'].keys()
	return pd.concat([index['ultindex'][i] for i in keys]).reset_index().to_dense().fillna(method='ffill').set_index('GEOHASH')['AREA'].to_dict()


# function for writing a json ot to a file
def make_json(dictionary,filename):
	with open(filename,'wb') as f:
		json.dump(dictionary,f)
	print 'Wrote Json.'

# function for reading json 
def read_json(filename):
	with open(filename,'rb') as f:
		return json.load(f)

# function for reading an h5 file containing an ultindex
def readh(filename):
	hdfsbool = False
	with pd.HDFStore(filename) as out:
		try:
			combined = out['combined']
			alignmentdf = out['alignmentdf']
			hdfsbool = True
		except:
			pass
	if hdfsbool ==True:
		newlist = []
		for row in combined.index.values.tolist():
			if not row == 'metadata' and not row == 'areamask':
				newlist.append(row)
		metadata = json.loads(combined[0].loc['metadata'])
		areamask = json.loads(combined[0].loc['areamask'])
		print metadata['output_type']
		if metadata['output_type'] == 'single':
			areamask1 = {}
			keys = [int(i) for i in areamask.keys()]
			areamask = dict(zip(keys,areamask.values()))
			ultindexjson = [json.loads(combined[0].loc[i]) for i in newlist]
			if len(ultindexjson) == 1:
				ultindex = ultindexjson[0]
		else:
			# regions here
			areamask1 = {}
			keys = [int(i) for i in areamask.keys()]
			areamask = dict(zip(keys,areamask.values()))
			ultindexjson = [json.loads(combined[0].loc[i]) for i in newlist]
			if len(ultindexjson) == 1:
				ultindex = ultindexjson[0]

			else:
				ultindex = dict(zip(newlist,ultindexjson))

		ultindex = {'alignmentdf':alignmentdf,
					'ultindex':ultindex,
					'areamask':areamask,
					'metadata':metadata}
	return ultindex

