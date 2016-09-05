import berrl as bl
import pandas as pd
import numpy as np
import geohash
import json
import itertools


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
	while ind == 0:
		current += 1

		# getting the current level of precision that is being tested
		ulcurrent = ulhash[:current]
		lrcurrent = lrhash[:current]

		# checking to see if values arent the same
		if not ulcurrent == lrcurrent:
			hashtable = bl.make_hashtable_range(ulcurrent,lrcurrent)
			shape = hashtable.shape
			if shape[0] > 3 and shape[1] > 3:
				ind = 1
	
	if maxsize == False:
		# setting the size of the proper ul hashs and lr hashs
		ulhash = ulhash[:current+2]
		lrhash = lrhash[:current+2]
	else:
		# setting the size of the proper ul hashs and lr hashs
		ulhash = ulhash[:maxsize]
		lrhash = lrhash[:maxsize]	

	# gettin corners
	ulhash = bl.get_corner(ulhash,'ul')
	lrhash = bl.get_corner(lrhash,'lr')


	# making the inner most hashtable needed
	hashtable = bl.make_hashtable_range(ulhash,lrhash)

	return hashtable

# getting index list
def get_indexlist(geohashlist,hashtablevals):
	hashtable = hashtablevals
	geohashs = geohashlist
	newlist = []
	for row in geohashlist:
		index = bl.get_index(hashtablevals,row)
		newlist.append(index)
	return newlist

# given a indexlist with [x,y] in each row return stringified indicy
# 'x,y'
def stringify_indicies(indexlist):
	newlist = []

	for row in indexlist:
		row = str(row[0]) + ',' + str(row[1])
		newlist.append(row)

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
	size = len(itable[itable['LATOUT'] > lat])
	# gettin innerbool
	if size / 2 == float(size) / 2.0 or size == 0:
		innerbool = False
	else:
		innerbool = True	
	return innerbool

# takes each point found and tests it against the vertical line test
def vert_line_test(data):
	global itable
	count = 0
	total = 0
	newlist = []
	for dfcolumn,itabletemp in data:
		itable = itabletemp
		
		dfcolumn['BOOL2'] = dfcolumn['LAT'].map(map_column)
		dfcolumn = dfcolumn[dfcolumn['BOOL2'] == True]
		
		newlist += dfcolumn['GEOHASH'].values.tolist()
		if count == 10:
			total += 10
			count = 0
			print '[%s / %s]' % (total,len(data))	
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
				print 'Progress: [%s/%s]' % (total,len(data))
				count = 0
		else:
			# gettin innerbool
			if len(temp) / 2 == float(len(temp)) / 2.0 or len(temp) == 0:
				newrow = row + [innerbool]
				newlist.append(newrow)				
			if count == 1000:
				total += count
				print 'Progress: [%s/%s]' % (total,len(data))
				count = 0

	return pd.DataFrame(newlist[1:],columns=newlist[0])


# gets each df associated with a specific x
# as well as the solved itabledf for each x
# returns a list of rows with [columndf,itablesolved]
def prepare_columns(data,itable):
	count = 0
	for row in data.columns.values.tolist():
		if 'lat' in str(row).lower():
			latpos = count
		elif 'long' in str(row).lower():
			longpos = count
		count += 1

	# getting x, and y values
	xy = data[0].str.split(',',expand=True)
	data[['X','Y']] = xy.astype(int)

	newlist = []
	for row in data['X'].unique():
		# getting temporary df that will be used for this column
		temp = data[data['X'] == row]
		
		# getting long value
		long = temp['LONG'].unique()[0]

		# getting lat outs on the itable
		itabletemp = itable
		itabletemp['LATOUT'] = ((long - itable['LONG1']) * itable['SLOPE']) + itable['LAT1']
		itabletemp = itabletemp[(((itabletemp['LONG1'] > long)&(itabletemp['LONG2'] < long))|((itabletemp['LONG1'] < long)&(itabletemp['LONG2'] > long)))]
		
		newlist.append([temp,itabletemp])
	return newlist


# mapping for expand geohashs
def map_points(point):
	lat,long,delta1,delta2 = geohash.decode_exactly(point)
	return str(lat) + ',' + str(long)

# expands out geohash table
def expand_geohashs(data2):
	#data2 = data2.unstack(level=0).reset_index()
	data3 = data2['GEOHASH'].map(map_points)
	#print data3,len(data3),len(data2)
	data2['STRING'] = data3
	data3 = data3.str.split(',',expand=True)
	data2[['LAT','LONG']] = data3.astype(float)
	return data2

# traverse columns rows for each unique value set in forward and reverse
def traverse_columns_rows(indexdf,indexlist,innerdf,alignmentdf):
	global totallist
	global innerhashdf
	innerhashdf = innerdf
	totallist = []

	# doing the column value iteration first
	cols = indexdf.columns.values.tolist()
	
	# iterating through each column
	for row in cols:
		temprow = indexdf[row].isin(indexlist)
		temprow = temprow[temprow == True]
		indextemp = temprow.index.values.tolist()
		if not indextemp == []:
			first,last = min(indextemp),max(indextemp)

			values1 = indexdf[row][:first]
			values2 = indexdf[row][last:]
			newrow = values1.values.tolist() + values2.values.tolist()

		else:
			newrow = indexdf[row].values.tolist()

		totallist += newrow

	# getting index values for iteration
	inds = indexdf.index.values.tolist()

	# iterating through each index value 
	for row in inds:
		temprow = indexdf.loc[row].isin(indexlist)
		temprow = temprow[temprow == True]
		indextemp = temprow.index.values.tolist()

		if not indextemp == []:
			first,last = min(indextemp),max(indextemp)

			values1 = indexdf.loc[row][:first]
			values2 = indexdf.loc[row][last:]
			newrow = values1.values.tolist() + values2.values.tolist()

		else:
			newrow = indexdf.loc[row].values.tolist()

		totallist += newrow

	# getting all the uniques in the totallist 
	totallist = np.unique(totallist).tolist()

	# getting first querry of bools to then check if within polygon 
	# via vertical line intersection test
	newdf = indexdf.unstack(level=0).reset_index()
	newdf['GEOHASH'] = innerdf.unstack(level=0).reset_index()[0]
	newdf['BOOL'] = newdf[0].isin(totallist)
	innergeohashs = newdf[newdf['BOOL'] == False]

	# vertical line intersection test
	itable = get_intersect_table(alignmentdf)
	#itable.to_csv('itable.csv',index=False)
	innergeohashs = expand_geohashs(innergeohashs)
	#innergeohashs.to_csv('innerhashs.csv',index=False)
	
	# preparing columns for vert_line_test
	data = prepare_columns(innergeohashs,itable)

	# getting innergeohashs
	innergeohashs = vert_line_test(data)
	innergeohashs = pd.DataFrame(innergeohashs,columns=['GEOHASH'])

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
	for key,value in kwargs.iteritems():
		if key == 'next_level':
			next_level = value
		if key == 'innerbool':
			innerbool = value

	# filling all the values in the alignment string list
	indexlist = [0]
	size = 3
	while len(indexlist) < 300 and maxsize == False:
		size += 1
		innerhashtable = get_inner_hashtable(alignmentdf,size)
		filleddf = fill_geohashs(alignmentdf,size)
		indexlist = get_indexlist(filleddf,innerhashtable)

	# getting index list if maxsize is already givven
	if not maxsize == False:
		innerhashtable = get_inner_hashtable(alignmentdf,maxsize)
		filleddf = fill_geohashs(alignmentdf,maxsize)
		indexlist = get_indexlist(filleddf,innerhashtable)
	
	# getting innerhash tables size
	corner = innerhashtable.loc[0][0]
	size = len(corner)

	if next_level == True:
		uniquegeohashs = np.unique(filleddf['GEOHASH']).tolist()
		totalhashs = make_unique_down(uniquegeohashs)
		next_level = check_third_dim(totalhashs,alignmentdf,innerbool)
	else:
		next_level = []	
	# getting indiceis table
	indexdf = bl.make_indicies(innerhashtable.shape[1],innerhashtable.shape[0])

	# stringifying indexlist
	strindexlist = stringify_indicies(indexlist)

	return traverse_columns_rows(indexdf,strindexlist,innerhashtable,alignmentdf),next_level

# gets the inner df of geohashs
def get_innerdf(data,maxsize,next_level):
	# getting the indexdf
	indexdf = bl.make_indicies(totalhashdf.shape[1],totalhashdf.shape[0])

	# getting the outer indicies of the df
	outerdf,next_level = get_innerhashs_outsided(data,maxsize,next_level=next_level)

	return outerdf,next_level

# function for getting inner hashs for an entire ring
def get_innerhashs_small(df,maxsize,**kwargs):
	next_level = False
	innerbool = False
	for key,value in kwargs.iteritems():
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

# checks thep oints
def check_third_dim(totalhashs,alignmentdf,innerbool):
	# getting dataframe for points
	data = bl.points_from_geohash(totalhashs)

	# creating intersect df for alignment table
	itable = get_intersect_table(alignmentdf)

	# checking each point exhaustive
	df = vert_line_test_exhaustive(data,itable,innerbool)
	return pd.DataFrame(df['GEOHASH'].values,columns=['total'])

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
	unstackedinner['COUNT'] = 1
	unstackedinner = unstackedinner.groupby(['GEOHASH1'])
	for name,group in unstackedinner:
		if len(group) == 32:
			total.append(name)
		else:
			total += group['GEOHASH'].values.tolist()

	total = pd.DataFrame(total,columns=['total'])
	return total
# converts indicies to string representation
# an applymap on all df function
def get_ind(geohash):
	global hashtable
	ind = bl.get_index(hashtable,geohash)

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

# takes geohash and a point returns percentage of point left to right and percentage point down
def fill_indicies(data,innerhashtable,precision):

	# function for linting whether the first point and lastpoint are the same if not appends talbe
	data = first_last(data)

	# getting the geohash table back
	data = bl.map_table(data,precision,map_only=True)

	# getting the table with ind partial positons back
	data = bl.ind_dec_points(data)

	global hashtable
	hashtable = innerhashtable

	
	# getting official indices adding the parital to the complete
	table = data['GEOHASH'].map(get_ind)
	table = table.str.split(',',expand=True)
	table.columns = ['x','y']
	data[['x']] = table['x'].astype(int) + data['x']
	data[['y']] = table['y'].astype(int) + data['y']

	newlist = []
	count = 0
	ind = False

	for row,b in itertools.izip(data[['x','y']].values.tolist(),data['GEOHASH'].values.tolist()):
		if count == 0:
			count = 1
			oldrow = row
		else:

			dist = distance(oldrow,row)
			dist = round(dist,0)
			if not dist == 0:
				points = generate_points(int(dist*3),oldrow,row)
				for row in points[1:]:
					x,y = round(row[0],0),round(row[1],0)
					x,y = int(x),int(y)
					try:
						b = hashtable[x][y]
 					except KeyError:
 						pass
 					if not [x,y] == oldrow or not oldhash == b:
						newlist.append([x,y])
						oldhash = b
						oldrow = [x,y]
			if not oldhash == b or not oldrow == row:
				x,y = round(row[0],0),round(row[1],0)
				x,y = int(x),int(y)
				newlist.append([x,y])
			
		oldrow = row
		oldhash = b 


	newlist2 = []
	for row in newlist:
		if count == 0:
			count = 1
			newlist2.append(row)
		else:
			if not oldrow == row:
				newlist2.append(row)
		oldrow = row
	return newlist2 



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
			if dist > hashsize / 10.0:
				number = (dist / hashsize) * 10.0
				number = int(number)
				newlist += generate_points(number,oldrow,row)[1:]
			else:
				newlist.append(row)
		oldrow = row

	newlist = pd.DataFrame(newlist,columns=['LONG','LAT'])
	newlist = bl.map_table(newlist,size,map_only=True)
	return newlist

# makes a geospatial index / dict structure to easily aggregate areas and other polygons
def make_index(data,**kwargs):
	output = False
	areaname = False
	maxsize = False
	rings = False
	next_level = False
	for key,value in kwargs.iteritems():
		if key == 'output':
			output = value
		if key == 'areaname':
			areaname = value
		if key == 'maxsize':
			maxsize = int(value)
		if key == 'rings':
			rings = value
		if key == 'next_level':
			next_level = value
	# logic for assembling a complex index with multiple rings
	if rings == True and not areaname == False:
		total = make_ring_index(data)
		total['area'] = areaname
		minsize = total['total'].str.len().min()
		maxsize = total['total'].str.len().max()
		
		# logic for turning the maxsize into hexidecimal to 
		# maintain one byte placing
		if maxsize > 9:
			maxsize = hex(maxsize)[2:]

		appendstr = str(minsize) + str(maxsize)	

		areaname = str(areaname) + '_' + appendstr
		if output == True:
			total.to_csv(str(areaname) + '.csv',index=False)
			return total
		else:
			return total
	elif not rings == False and not areaname == False:
		total = make_single_ring(data,rings,next_level=next_level)
		total['area'] = areaname
		minsize = total['total'].str.len().min()
		maxsize = total['total'].str.len().max()
		
		# logic for turning the maxsize into hexidecimal to 
		# maintain one byte placing
		if maxsize > 9:
			maxsize = hex(maxsize)[2:]

		appendstr = str(minsize) + str(maxsize)	

		areaname = str(areaname) + '_' + appendstr
		if output == True:
			total.to_csv(str(areaname) + '.csv',index=False)
			return total
		else:
			return total


	areadf,next_level = get_innerhashs_small(data,maxsize,next_level=next_level)
	total = assemble_outputs(areadf)
	if not len(next_level) == 0:
		total = pd.concat([total,next_level])
		total = filter_lint_data3d(total,data,[])
	return total


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
	for key,value in kwargs.iteritems():
		if key == 'next_level':
			next_level = value

	# logic to account for if no inner rings are given
	if len(inner_rings) == 0:
		return []

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
		bl.make_line(newtemp,filename=str(count)+'.geojson')
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

# when drilling down one more layer some data becomes invalidated
# this is a few dataframe abstractions to get the correct geohashs out of the data
def filter_lint_data3d(data,outeralignmentdf,rings):
	# filter data from geohashs one layer above me
	# its own alg itself basically
	data = filter_relevant_nextdim(data)

	# getting the maximum geohash sizze
	highest = data['total'].str.len().max()
	
	# getting all the unique values at the highest leveel geohash with a string slice applied
	temp = np.unique(data[(data['total'].str.len() == highest)]['total'].str[:-2]).tolist()

	# doing an isin operation on the smallest size geohashs in our current layout 
	# this bool does an is in about the list created above to create a slicing column
	data['BOOL'] =data[data['total'].str.len()==highest-2]['total'].isin(temp)
	data = data.fillna(value=False)

	# getting all highest level (largest) geohashs that need to bel linted
	b = data[data['BOOL'] == True]
	

	othervalues = data[data['BOOL'] == False]
	othervalues = np.unique(othervalues['total']).tolist()
	
	# getting the unique geohashs extening them into a points dataframe 
	# this dataframe will be compared against the outer rings as well as 
	# the outer ring
	uniquegeohashs = b['total'].values.tolist()
	expandedgeohashs = make_unique_down(uniquegeohashs)
	potpoints = bl.points_from_geohash4(expandedgeohashs)

	# creating intersect df for alignment table
	itable = get_intersect_table(outeralignmentdf)

	# checking each point exhaustive
	df = vert_line_test_exhaustive(potpoints,itable,True)
	totalgeohashs = np.unique(df['GEOHASH']).tolist()
	for row in rings:

		# creating intersect df for alignment table
		itable = get_intersect_table(row)
		df = vert_line_test_exhaustive(potpoints,itable,False)
		totalgeohashs += np.unique(df['GEOHASH']).tolist()

	# creating final bool for an isin statement about geohashs
	potpoints['BOOL2'] = potpoints['GEOHASH'].isin(totalgeohashs)
	potpoints = potpoints.fillna(value=False)
	geohashsdesired = potpoints[potpoints['BOOL2'] == False]


	totalhashs = np.unique(geohashsdesired['GEOHASH']).tolist() + othervalues

	return pd.DataFrame(totalhashs,columns=['total'])

# function for creatng an index of a single object with multiple ring levels
# called from make_index
def make_single_ring(outer_ring,inner_rings,**kwargs):
	next_level = False
	for key,value in kwargs.iteritems():
		if key == 'next_level':
			next_level = value

	#make_poly_lines(temp,uniquealigns)

	# getting outer geohashs that will encompass ring
	geohashsouter,nextlevel1 = get_innerhashs_small(outer_ring,False,next_level=next_level)
	print nextlevel1,'nextlevel1'
	# getting maxsize of geohashs from the df just made
	maxsize = geohashsouter['GEOHASH'].str.len().max()

	# getting the list of geohash holes
	geohashholes,nextlevel2 = get_innerring_tables(inner_rings,maxsize,next_level=next_level)
	print nextlevel2,'2'
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
def make_ring_index(data):
	# adding the data column ring to df	
	ringhole = data['PART'].str.split('_',expand=True)
	data[['RING','HOLE']] = ringhole
	uniques = np.unique(data['RING']).tolist()

	# iterating through each unique ring
	count = 0
	totals = []
	for row in uniques:
		temp = data[data['RING'] == str(row)]
		uniquealigns = np.unique(temp['PART'])
		outer_ring,inner_rings = get_outer_inner(temp,uniquealigns)
		#make_poly_lines(temp,uniquealigns)
		total = make_single_ring(outer_ring,inner_rings)
		totals.append(total)

		count += 1
	totals = pd.concat(totals)
	return totals

'''
data = pd.read_csv('a.csv')
bl.clean_current()
make_index(data,rings=True,areaname='aa',output=True)
total = pd.read_csv('aa.csv')

geohashs = total['total'].values.tolist()
blocks = bl.make_geohash_blocks(geohashs)
bl.make_blocks(blocks,filename='blocks.geojson')

bl.a()
'''
