import pandas as pd
import numpy as np
import geohash
import json
import itertools
import time
from pipegeohash import map_table
from geohash_logic import get_corner
import future

# gets the extrema dictionary of the alignment df
def get_extrema(df):
	# getting lat and long columns
	for row in df.columns.values.tolist():
		if 'lat' in str(row).lower():
			latheader = str(row)
		if 'lon' in str(row).lower():
			longheader = str(row)
	
	# getting n,s,e,w extrema
	south,north = df[latheader].min(),df[latheader].max()
	west,east = df[longheader].min(),df[longheader].max()

	# making dictionary for extrema
	extrema = {'n':north,'s':south,'e':east,'w':west}

	return extrema


# creates a range of lats / longs then zips
def create_range(val1,val2):
	if val1 > val2:
		minval = val2
		maxval = val1
	else:
		minval = val1
		maxval = val2

	delta = (maxval - minval) / float(600)
	newlist = [minval]
	current = minval
	for i in range(600):
		current += delta
		newlist.append(current)
	newlist.append(maxval)
	return newlist


def map_table_new(data,precision=False,return_geohashs=False):
	for row in data.columns.values.tolist():
		if 'lat' in str(row).lower():
			lathead = row
		elif 'long' in str(row).lower():
			longhead = row

	# filtering all the inapplicable data
	data = data[(data[lathead] < 90.0) & (data[lathead] > -90.0)]
	myfunc = np.vectorize(geohash.encode)


	if return_geohashs == True:
		if precision == False:
			myfunc(data[lathead].values,data[longhead].values)
		else:
			myfunc(data[lathead].values,data[longhead].values,precision)
	else:
		if precision == False:
			data['GEOHASH'] = myfunc(data[lathead].values,data[longhead].values)
		else:
			data['GEOHASH'] = myfunc(data[lathead].values,data[longhead].values,precision)

	return data


def make_points_geohash(c1,c2,size):
	lat1,long1,latdelta,longdelta = geohash.decode_exactly(c1)
	lat2,long2 = geohash.decode(c2)

	latdelta,longdelta = latdelta * 2,longdelta * 2
	
	# creating lats and longs
	longs = np.linspace(long1,long2,(abs(long2 - long1) / longdelta) + 1)
	lats = np.linspace(lat1,lat2,(abs(lat2 - lat1) / latdelta) + 1)



	total = []
	count = 0
	for long in longs:
		total += zip(lats,[long] * len(lats),[count] * len(lats))
		count += 1
	total = pd.DataFrame(total,columns=['LAT','LONG','X'])

	return map_table_new(total,precision=size)


# getting the outermost precision in which blocks can fit within bounds
def get_inner_hashtable(df,maxsize):
	# getting extrema
	extrema = get_extrema(df)

	# getting upper lefft and lowerright point
	ul = [extrema['w'],extrema['n']]
	lr = [extrema['e'],extrema['s']]
	# getting geohash for ul and lr
	# assuming 8 for the time being as abs min
	ulhash = geohash.encode(ul[1],ul[0],12)
	lrhash = geohash.encode(lr[1],lr[0],12)


	# setting while loop to continue iterating until a dataframe of shape
	# bigger than 3x3 is returned
	ind = 0
	current = 0
		
	if maxsize == False:
		# setting the size of the proper ul hashs and lr hashs
		ulhash = ulhash[:current+2]
		lrhash = lrhash[:current+2]
	else:
		# setting the size of the proper ul hashs and lr hashs
		ulhash = ulhash[:maxsize]
		lrhash = lrhash[:maxsize]	

	# gettin corners
	ulhash = get_corner(ulhash,'ul')
	lrhash = get_corner(lrhash,'lr')

	# making the inner most hashtable needed
	hashtable = make_points_geohash(ulhash,lrhash,maxsize)

	return hashtable

# getting index list
def get_indexlist(geohashlist,hashtablevals):
	hashtable = hashtablevals
	geohashs = geohashlist
	newlist = []
	for row in geohashlist:
		index = get_index(hashtablevals,row)
		newlist.append(index)
	return newlist


# gets the alignment df table used to compare the horiztonal line test
def get_intersect_table(data):
	# getting lat and long columns
	for row in data.columns.values.tolist():
		if 'lat' in str(row).lower():
			latheader = str(row)
		if 'lon' in str(row).lower():
			longheader = str(row)

	listdata = data[[longheader,latheader]].values.tolist()
	oldrow = listdata[-1]
	newlist = []
	for row in listdata:
		x1,x2 = row[0],oldrow[0]
		y1,y2 = row[1],oldrow[1]
		if not x1 - x2 == 0:
			slope = (y1 - y2) / (x1 - x2)
		else:
			slope = 1000000000
		newlist.append(row+oldrow+[slope])
		oldrow = row
	data = pd.DataFrame(newlist,columns=['LONG1','LAT1','LONG2','LAT2','SLOPE'])

	return data


# given a latout compares agaisnt global itable
# returns true or false based on that
def map_column(lat):
	if not icomparison == False:
		minlat,maxlat = icomparison
		if minlat < lat and maxlat > lat:
			return True
		else:
			return False
	size = len(itable[itable['LATOUT'] > lat])
	# gettin innerbool
	if size / 2 == float(size) / 2.0 or size == 0:
		return False
	else:
		return True	

# takes each point found and tests it against the vertical line test
def vert_line_test(data):
	global itable
	global icomparison
	count = 0
	total = 0
	newlist = []
	for dfcolumn,itabletemp,comparison in data:
		itable = itabletemp
		icomparison = comparison
		if len(itable) == 0:
			dfcolumn['BOOL2'] = False
		else:
			dfcolumn['BOOL2'] = dfcolumn['LAT'].map(map_column)
			dfcolumn = dfcolumn[dfcolumn['BOOL2'] == True]
		
		newlist += dfcolumn['GEOHASH'].values.tolist()
		if count == 10:
			total += 10
			count = 0
			print('[%s / %s]' % (total,len(data)))
		count += 1	
	return newlist

# takes each point found and tests it against the vertical line test
# this line test test the outer extremy geohashs flat
# only used if 3rd dimmension is desired
def vert_line_test_exhaustive(data,itable,innerbool):
	count = 0
	for row in data.columns.values.tolist():
		if 'lat' in str(row).lower():
			latpos = count
		elif 'long' in str(row).lower():
			longpos = count
		count += 1
	header = data.columns.values.tolist() + ['BOOL2']
	newlist = [header]
	count = 0
	total = 0
	for row in data.values.tolist():
		count += 1
		lat,long = row[latpos],row[longpos]
		itable['LATOUT'] = ((long - itable['LONG1']) * itable['SLOPE']) + itable['LAT1']
		
		# setting the LONGOUT
		itable['LONGOUT'] = long

		temp = itable[(((itable['LONG1'] > long)&(itable['LONG2'] < long))|((itable['LONG1'] < long)&(itable['LONG2'] > long)))&(itable['LATOUT'] > lat)]
		

		if innerbool == False:
			# gettin innerbool
			if not len(temp) / 2 == float(len(temp)) / 2.0:
				newrow = row + [innerbool]
				newlist.append(newrow)

			if count == 1000:
				total += count
				print('Progress: [%s/%s]' % (total,len(data)))
				count = 0
		else:
			# gettin innerbool
			if len(temp) / 2 == float(len(temp)) / 2.0 or len(temp) == 0:
				newrow = row + [innerbool]
				newlist.append(newrow)				
			if count == 1000:
				total += count
				print('Progress: [%s/%s]' % (total,len(data)))

				count = 0

	return pd.DataFrame(newlist[1:],columns=newlist[0])

# makes bin / labes for the pd.cut operation
# assembling a range and creating labels
def make_bins_labels(dictthing):
	newdict = {}
	for name in sorted(dictthing.keys()):	
		rawlats = dictthing[name]
		bins = []
		count = 0
		rawlats = sorted(np.unique(rawlats).tolist())
		count2 = 0
		for row in rawlats[1:]:
			if count == 0:
				bins.append('in' + str(count2))
				count = 1
			elif count == 1:
				bins.append('out' + str(count2))
				count = 0
			count2 += 1

		if rawlats == []:
			rawlats = [0]
		newdict[name] = [rawlats,bins]
	return newdict

# solving alignment for 
# the best method of solving all equations
def solve_alignment(longs,coords):
	count = 0
	newlist = []
	dictthing = {}
	if not coords[0] == coords[-1]:
		coords = coords + [coords[0]]
	for long,lat in coords:
		if count == 0:
			count = 1
		else:
			current = 0
			ind = 0
			for row in longs:
				ind = 0
				if row < long and row > oldlong:
					ind = 1
				elif row < oldlong and row > long:
					ind = 1
				if ind == 1:
					if oldlong == long:
						slope == 100000000
					else:
						slope = (lat - oldlat) / (long - oldlong)
					lat1 = ((row - oldlong) * slope) + oldlat
					try:
						dictthing[current].append(lat1)
					except:
						dictthing[current] = [lat1]
				#solves['LAT'] = ((solves['LONG'] - oldlong) * slope) + oldlat
				#solves['X'] = solves.index + 1
				current = current + 1

		oldlong = long
		oldlat = lat
	dictthing = make_bins_labels(dictthing)
	return dictthing

def testfunc(data):
	global solves
	name = data.name
	try: 
		rawlats,labels = solves[name]
	except:
		rawlats,labels = [0],[]
	return pd.cut(data,bins=rawlats,labels=labels)


# gets each df associated with a specific x
# as well as the solved itabledf for each x
# this is where the point in polygon solves are done entirely
def prepare_columns(data,dictthing):
	global solves
	solves = dictthing
	count = 0
	for row in data.columns.values.tolist():
		if 'lat' in str(row).lower():
			latpos = count
		elif 'long' in str(row).lower():
			longpos = count
		count += 1
	count3 = 0
	newlist = []
	total = 0
	xsize = len(np.unique(data['LONG']))
	data['LABEL'] = data.groupby('X')['LAT'].apply(testfunc)
	data = data[data['LABEL'].astype(str).str[:2] == 'in']
	return data['GEOHASH']

# mapping for expand geohashs
def map_points(point):
	lat,long,delta1,delta2 = geohash.decode_exactly(point)
	return str(lat) + ',' + str(long)

# expands out geohash table
def expand_geohashs(data2):
	#data2 = data2.unstack(level=0).reset_index()
	data3 = data2['GEOHASH'].map(map_points)
	data2['STRING'] = data3
	data3 = data3.str.split(',',expand=True)
	data2[['LAT','LONG']] = data3.astype(float)

	return data2
# expands out geohash table
def add_latlngs(data):
	newlist = []
	lats =[]
	longs = []
	for row in data['GEOHASH']:
		lat,long = geohash.decode(row)
		longs.append(long)
		lats.append(lat)
	data['LAT'] = lats
	data['LONG'] = longs

	return data

# traverse columns rows for each unique value set in forward and reverse
def traverse_columns_rows(innerdf,alignmentdf):
	global totallist
	global innerhashdf
	totallist = []
	# vertical line intersection test

	#s = time.time()
	solves = solve_alignment(np.unique(innerdf['LONG']).tolist(),alignmentdf[['LONG','LAT']].values.tolist())
	
	ids = sorted([int(i) for i in solves.keys()])

	#itable.to_csv('itable.csv',index=False)
	#innergeohashs.to_csv('innerhashs.csv',index=False)
	# preparing columns for vert_line_test
	innerdf = prepare_columns(innerdf,solves)

	# getting innergeohashs
	innergeohashs = pd.DataFrame(innerdf,columns=['GEOHASH'])
	return innergeohashs

def get_indexlist(filleddf,innerhashtable):
	innerhashtable = innerhashtable.unstack(level=0).reset_index()
	uniquegeohashs = np.unique(filleddf).tolist()
	innerhashtable['BOOL'] = innerhashtable[0].isin(uniquegeohashs)
	innerhashtable = innerhashtable[innerhashtable['BOOL'] == True]
	return innerhashtable[['level_0','level_1']].values.tolist()


# gets the inner geohashs outside the alignment data table given
def get_innerhashs_outsided(alignmentdf,maxsize,**kwargs):
	next_level = False
	innnerbool = False
	for key,value in kwargs.items():
		if key == 'next_level':
			next_level = value
		if key == 'innerbool':
			innerbool = value

	# filling all the values in the alignment string list
	indexlist = [0]
	size = 3
	alignsize = 0
	while alignsize < 300 and maxsize == False:
		size += 1
		#innerhashtable = get_inner_hashtable(alignmentdf,size)
		filleddf = fill_geohashs(alignmentdf,size)
		alignsize = len(np.unique(filleddf['GEOHASH']))
		#indexlist = get_indexlist(filleddf,innerhashtable)

	#innerhashtable = get_inner_hashtable(alignmentdf,size)
	innerhashtable = get_inner_hashtable(alignmentdf,size)

	# getting index list if maxsize is already givven
	if not maxsize == False:
		innerhashtable = get_inner_hashtable(alignmentdf,maxsize)
		filleddf = fill_geohashs(alignmentdf,maxsize)
	
	if next_level == True:
		uniquegeohashs = np.unique(filleddf['GEOHASH']).tolist()
		totalhashs = make_unique_down(uniquegeohashs)
		next_level = check_third_dim(totalhashs,alignmentdf,innerbool)
	else:
		next_level = []	

	#df = add_latlngs(df)
	return traverse_columns_rows(innerhashtable,alignmentdf),next_level

# gets the inner df of geohashs
def get_innerdf(data,maxsize,next_level):
	# getting the indexdf
	indexdf = make_indicies(totalhashdf.shape[1],totalhashdf.shape[0])

	# getting the outer indicies of the df
	outerdf,next_level = get_innerhashs_outsided(data,maxsize,next_level=next_level)

	return outerdf,next_level

# function for getting inner hashs for an entire ring
def get_innerhashs_small(df,maxsize,**kwargs):
	next_level = False
	innerbool = False
	for key,value in kwargs.items():
		if key == 'next_level':
			next_level = value
		if key == 'innerbool':
			innerbool = value

	# function for gettting maxsize
	if maxsize == False:
		innergeohashs,next_level = get_innerhashs_outsided(df,maxsize,next_level=next_level,innerbool=innerbool)
	else:
		innerhashtable = get_inner_hashtable(df,maxsize)
		innergeohashs,next_level = get_innerhashs_outsided(df,maxsize,next_level=next_level,innerbool=innerbool)
	return innergeohashs,next_level

# for all the uniques that are given as list
# of geohashs a simple and fast function to append 
# all 32 byte operands to the next level for each unique 
def make_unique_down(uniquegeohashlist):
	totalhashs = []
	for row in uniquegeohashlist:
		currenthash = row
		for row in '0123456789bcdefghjkmnpqrstuvwxyz':
			newhash = currenthash + row
			totalhashs.append(newhash)
	return totalhashs

# assembles the final outputs from a geohash df of inner small geohashs
def assemble_outputs(unstackedinner,**kwargs):
	#unstackedinner.to_csv('unstacked.csv',index=False)
	return_total = False
	# kwarg code block

	# setting initial outputs that will be appended or added
	total = []
	innergeohashs = []
	outerdict = {}

	# creating lower column and summing totals
	unstackedinner['GEOHASH1'] = unstackedinner['GEOHASH'].str[:-1]
	unstackedinner1 = unstackedinner
	unstackedinner = unstackedinner.groupby(['GEOHASH1']).count().reset_index()
	uniques = unstackedinner[unstackedinner['GEOHASH'] == 32]['GEOHASH1'].values.tolist()
	unstackedinner1['BOOL'] = unstackedinner1['GEOHASH1'].isin(uniques)
	
	total = pd.DataFrame(uniques + unstackedinner1[unstackedinner1['BOOL'] == False]['GEOHASH'].values.tolist(),columns=['total'])
	return total

# converts indicies to string representation
# an applymap on all df function
def get_ind(geohash):
	global hashtable
	ind = get_index(hashtable,geohash)
	return str(ind[0]) + ',' + str(ind[1])

# given a point1 x,y and a point2 x,y returns distance in miles
# points are given in long,lat geospatial cordinates
def distance(point1,point2):
	point1 = np.array(point1)
	point2 = np.array(point2)
	return np.linalg.norm(point1-point2)

# returns a set of points that traverse the linear line between two points
def generate_points(number_of_points,point1,point2):
	# getting x points
	x1,x2 = point1[0],point2[0]
	xdelta = (float(x2) - float(x1)) / float(number_of_points)
	xcurrent = x1

	# getting y points
	y1,y2 = point1[1],point2[1]
	ydelta = (float(y2) - float(y1)) / float(number_of_points)
	ycurrent = y1

	newlist = [['LONG','LAT']]

	count = 0
	while count < number_of_points:
		count += 1
		xcurrent += xdelta
		ycurrent += ydelta
		newlist.append([xcurrent,ycurrent])

	return newlist


# checks the first and last value of an alignment dataset
# and fixes it if required
def first_last(data):
	first = data[:1]
	last = data[-1:]

	for row in first.columns.values.tolist():
		if 'lat' in  str(row).lower():
			latheader = row
		elif 'long' in  str(row).lower():
			longheader = row
	
	# getting first and last point
	firstpoint = [first[latheader].values.tolist()[0],first[longheader].values.tolist()[0]]
	lastpoint = [last[latheader].values.tolist()[0],last[longheader].values.tolist()[0]]

	# if these are not the same concatenating the current data table with first point
	if not firstpoint == lastpoint:
		return pd.concat([data,first])
	else:
		return data


# hopefully a function can be made to properly make into lines
def fill_geohashs(data,size):
	# function for linting whether the first point and lastpoint are the same if not appends talbe
	data = first_last(data)

	
	extrema = get_extrema(data)

	# getting upper lefft and lowerright point
	ul = [extrema['w'],extrema['n']]
	lr = [extrema['e'],extrema['s']]


	# getting geohash for ul and lr
	# assuming 8 for the time being as abs min
	ulhash = geohash.encode(ul[1],ul[0],size)
	lrhash = geohash.encode(lr[1],lr[0],size)

	lat,long,latdelta,longdelta = geohash.decode_exactly(ulhash)

	latdelta,longdelta = latdelta * 2.0,longdelta * 2.0

	hashsize = ((latdelta ** 2) + (longdelta ** 2)) ** .5

	count = 0
	for row in data.columns.values.tolist():
		if 'lat' in str(row).lower():
			latheader = row
		elif 'long' in str(row).lower():
			longheader = row
		count += 1


	count = 0
	newlist = []
	for row in data[[longheader,latheader]].values.tolist():
		if count == 0:
			count = 1
		else:
			dist = distance(oldrow,row)
			if dist > hashsize / 5.0:
				number = (dist / hashsize) * 5.0
				number = int(number)
				newlist += generate_points(number,oldrow,row)[1:]
			else:
				newlist.append(row)
		oldrow = row

	newlist = pd.DataFrame(newlist,columns=['LONG','LAT'])
	newlist = map_table(newlist,size,map_only=True)
	return newlist

def indexs_tofiles(total,inner,outerdict):
	with open('areadict.json','wb') as newgeojson:
		json.dump(outerdict,newgeojson)

	inner = pd.DataFrame(inner,columns=['inner'])
	inner.to_csv('inner.csv',index=False)

	total = pd.DataFrame(total,columns=['total'])
	total.to_csv('total.csv',index=False)

def read_all():
	with open('areadict.json') as data_file:    
		data = json.load(data_file)
	inner = pd.read_csv('inner.csv')
	total = pd.read_csv('total.csv')

	return data,inner,total

# given a df containing one ring returns outer ring and inner rings
def get_outer_inner(df,uniques):
	inner = []
	for row in uniques:
		temp = df[df['PART'] == str(row)]
		if not '_h' in str(row):
			outerring = temp
		else:
			inner.append(temp)
	return outerring,inner

# get inner hashs after getting the otuer hashs
# this function specifically takes a whole alignments
def get_innerring_tables(inner_rings,maxsize,**kwargs):
	next_level = False
	next_levels = []
	for key,value in kwargs.items():
		if key == 'next_level':
			next_level = value

	# logic to account for if no inner rings are given
	if len(inner_rings) == 0:
		return [],[]

	# iterating through each ring
	newlist = []
	for row in inner_rings:
		geohashs,next = get_innerhashs_small(row,maxsize,next_level=next_level,innerbool=True)
		newlist.append(geohashs)
		if not len(next) == 0:
			next_levels.append(next)
	if not len(next_levels) == 0:
		next_level = pd.concat(next_levels)
	else:
		next_level = []
	return newlist,next_level

# function to make each alignment as a line
def make_poly_lines(temp,aligns):
	count = 0
	for row in aligns:
		newtemp = temp[temp['PART'] == str(row)]
		make_line(newtemp,filename=str(count)+'.geojson')
		count += 1


# this function essentially does the equilvalent of a assemble 
# outputs but sort of after the fact linting some of the data 
# found to be no longer valid
def filter_relevant_nextdim(data):
	highest = data['total'].str.len().max()
	temp = np.unique(data[(data['total'].str.len() == highest)]['total'].str[:-1]).tolist()
	data['BOOL'] = data[data['total'].str.len()==highest-1]['total'].isin(temp)
	data = data.fillna(value=False)
	data = data[((data['total'].str.len()==highest-1)&(data['BOOL']==False))|(data['total'].str.len() == highest)|(data['total'].str.len() == highest-2)]
	return data

# function for creatng an index of a single object with multiple ring levels
# called from make_index
def make_single_ring(outer_ring,inner_rings,**kwargs):
	next_level = False
	for key,value in kwargs.items():
		if key == 'next_level':
			next_level = value

	#make_poly_lines(temp,uniquealigns)

	# getting outer geohashs that will encompass ring
	geohashsouter,nextlevel1 = get_innerhashs_small(outer_ring,False,next_level=next_level)
	# getting maxsize of geohashs from the df just made
	maxsize = geohashsouter['GEOHASH'].str.len().max()

	# getting the list of geohash holes
	geohashholes,nextlevel2 = get_innerring_tables(inner_rings,maxsize,next_level=next_level)
	# logic for handling if no holes were given
	if len(geohashholes) == 0:
		totalgeohashs = geohashsouter
	else:
		totalgeohashs = pd.concat([geohashsouter]+geohashholes)
	
	if not len(geohashholes) == 0:
		# doing the groupby and selecting single entry geohashs
		totalgeohashs['COUNT'] = 1
		totalgeohashs = totalgeohashs[['GEOHASH','COUNT']].groupby('GEOHASH').sum()
		totalgeohashs = totalgeohashs.reset_index()

		# selecting only the single entry geohashs
		totalgeohashs = totalgeohashs[totalgeohashs['COUNT'] == 1]
	
	total = assemble_outputs(totalgeohashs,return_total=True)

	if not len(nextlevel1) == 0 and not len(nextlevel2) == 0:
		total = pd.concat([total,nextlevel1,nextlevel2])
		total = filter_lint_data3d(total,outer_ring,inner_rings)
	return total


# iterates through each unique ring series
# selecting part of the alignment frame for each
def make_ring_index(data,printoff=False):
	# adding the data column ring to df	
	ringhole = data['PART'].str.split('_',expand=True)
	data[['RING','HOLE']] = ringhole
	uniques = np.unique(data['RING']).tolist()

	# iterating through each unique ring
	count = 0
	totals = []
	for row in uniques:
		s = time.time()
		temp = data[data['RING'] == str(row)]
		uniquealigns = np.unique(temp['PART'])
		outer_ring,inner_rings = get_outer_inner(temp,uniquealigns)
		#make_poly_lines(temp,uniquealigns)
		total = make_single_ring(outer_ring,inner_rings)
		totals.append(total)

		count += 1
		if printoff == False:
			print('Rings within Area: [%s/%s] & Ring Time: %s' % (count,len(uniques),time.time()-s))
	totals = pd.concat(totals)
	return totals

