import pandas as pd
import numpy as np
import itertools
import geohash
import time
import json
from pipegeohash import map_table,list2df,df2list
import deepdish as dd
from polygon_dict import merge_dicts
from geohash_logic import *

# gets the extrema dictionary of the alignment df
def get_extrema(df):
	if isinstance(df,list):
		w = 1000
		e = -1000
		n = -1000
		s = 1000
		for long,lat,valid in df:
			if long < w:
				w = long
			if long > e:
				e = long
			if lat > n:
				n = lat
			if lat < s:
				s = lat
		return {'n':n,'s':s,'e':e,'w':w}

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


# returns a set of points that traverse the linear line between two points
def generate_points_geohash(number_of_points,point1,point2,areaindex,size):
	# getting x points
	geohashlist = []

	x1,x2 = point1[0],point2[0]
	xdelta = (float(x2) - float(x1)) / float(number_of_points)
	xcurrent = x1

	# getting y points
	y1,y2 = point1[1],point2[1]
	ydelta = (float(y2) - float(y1)) / float(number_of_points)
	ycurrent = y1
	geohashlist = ['GEOHASH',geohash.encode(y1,x1,size)]

	count = 0
	while count < number_of_points:
		count += 1
		xcurrent += xdelta
		ycurrent += ydelta
		geohashlist.append(geohash.encode(ycurrent,xcurrent,size))
	geohashlist.append(geohash.encode(point2[1],point2[0],size))
	return geohashlist

# given a point1 x,y and a point2 x,y returns distance in miles
# points are given in long,lat geospatial cordinates
def distance(point1,point2):
	point1 = np.array(point1)
	point2 = np.array(point2)
	return np.linalg.norm(point1-point2)



def get_cords_json(coords):
	data = '{"a":%s}' % coords.decode('utf-8') 
	data = json.loads(data)	
	return data['a']

# given a geohash list returns a list of geohashs with
# the neighbors added to eachvalue
def make_neighbors(geohashlist,firstlast=False):
	if firstlast == True:
		newlist = []
		for row in geohashlist:
			add = geohash.neighbors(row)
			newlist += add
		newlist = np.unique(newlist).tolist()
		return newlist

	first = geohashlist[0]
	newlist = [first]
	last = geohashlist[-1]
	for ghash in geohashlist[1:-1]:
		add = geohash.neighbors(ghash)
		newlist += add
	newlist.append(last)
	newlist = np.unique(newlist).tolist()
	return newlist


# hopefully a function can be made to properly make into lines
def fill_geohashs(data,name,size):
	# function for linting whether the first point and lastpoint are the same if not appends talbe
	hashsize = get_hashsize(data[0],size)

	count = 0
	geohashlist = []
	tangslist = []
	currentdist = 0.
	for row in data:
		if count == 0:
			count = 1
		else:
			slope = get_slope(oldrow,row)
			x1,y1 = oldrow
			dist = distance(oldrow,row)
			if dist > hashsize / 5.0:
				number = (dist / hashsize) * 5.0
				number = int(number)
				addghashs = generate_points_geohash(number,oldrow,row,name,size)[1:]
				addghashs = np.unique(addghashs).tolist()
				addghashs = [interpolate_hash(x1,y1,ghash,slope,currentdist,name) for ghash in addghashs]				
				geohashlist += addghashs
			else:
				point = row
			currentdist += dist

		oldrow = row

	

	return geohashlist

def interpolate_hash(x1,y1,ghash,slope,currentdist,name):
	lat,x = geohash.decode(ghash)
	xdelta = x - x1
	y = (xdelta * slope) + y1
	pt = [x,y]
	current = currentdist + distance(pt,[x1,y1])
	return [ghash,current,name]

# hopefully a function can be made to properly make into lines
def create_distances(data,name,size):
	# function for linting whether the first point and lastpoint are the same if not appends talbe
	hashsize = get_hashsize(data[0],size)

	count = 0
	geohashlist = []
	tangslist = []
	uniques = []
	neighbors = []
	currentdist = 0
	for row in data:
		if count == 0:
			count = 1
		else:
			slope = get_slope(oldrow,row)
			x1,y1 = oldrow 
			dist = distance(oldrow,row)
			if dist > hashsize / 5.0:
				number = (dist / hashsize) * 5.0
				number = int(number)
				add_dist = generate_points_geohash(number,oldrow,row,name,size)[1:]
				add_dist = np.unique(add_dist).tolist()
				uniques += add_dist
				add_dist = [interpolate_hash(x1,y1,ghash,slope,currentdist) for ghash in add_dist]
				geohashlist += add_dist
			else:
				point = row
				lastpoint = geohash.encode(point[1],point[0],size)
			currentdist += dist

		oldrow = row

	return geohashlist,neighbors

# making flat list non sorted
def flatten_nonsorted(data):
	ghashs = data['GEOHASH']
	indexes = np.unique(ghashs, return_index=True)[1]
	list = [ghashs[index] for index in sorted(indexes)]
	return list	

# makes a line mask for the lineids of a given df
def make_line_mask(data):
	linemask1 = {}
	linemask2 = {}
	uniques = np.unique(data['gid']).tolist()
	for i,unique in itertools.izip(range(len(uniques)),uniques):
		key = str(hex(i))[2:]
		linemask2[key] = unique
		linemask1[unique] = key

	return linemask1,linemask2

def make_sum(i):
	iis = zip(geohash.neighbors(i[0]),[i[0]] * 9)
	return '|'.join(['%s,%s' % (i[0],i[1]) for i in iis])

# making line segment index
def make_line_index(data,outfilename,**kwargs):
	data1 = data
	csv = False
	uniqueid = False
	filename = False
	return_index = False
	precision = 9
	for key,value in kwargs.iteritems():
		if key == 'csv':
			csv = value
		if key == 'filename':
			filename = value
		if key == 'uniqueid':
			uniqueid = value
		if key == 'return_index':
			return_index = value
		if key == 'precision':
			precision = value

	if uniqueid == False:
		uniqueidheader = 'gid'
	else:
		uniqueidheader = uniqueid
	if filename == False:
		filename = 'line_index.csv'

	# logic for whether or not the maxdistance
	# has to be derived or can it be infered from a columnfield
	maxbool = False
	for row in data.columns.values.tolist():
		if str(row).lower() == 'maxdistance':
			maxbool = True

	# logic for getting the dataframe needed to create the 
	# distance dictionary
	if maxbool == True:
		dicttable = data.set_index('gid')
		segdistance = dicttable['maxdistance'].to_dict()

	# getting header and intializing
	header = data.columns.values.tolist()
	totalgeohashs = []
	totalids = []
 	count = 0
 	total = 0
 	# getting unique column position
 	for row in header:
 		#if headercolumn in str(row):
 		#	position = count
 		row = row.encode('utf-8')
 		if uniqueidheader in str(row):
 			uniqueidrow = count
 		count += 1

 	linemask1,linemask2 = make_line_mask(data)

 	count = 0
 	geohashlist = []
 	neighborslist = []
 	distances = []
 	neighdists = []
 	for coords,row in itertools.izip(data['coords'].map(get_cords_json).values.tolist(),data.values.tolist()):
		#L1 = coords
		#L2 = [[linemask1[row[uniqueidrow]]]] * len(coords)
		#coords = [x + y for x,y in zip(L1,L2)]
		#newdata = pd.DataFrame(row,columns=['LONG','LAT'])
		name = linemask1[row[uniqueidrow]]
		addgeohashs = fill_geohashs(coords,name,precision)
		#print len(addgeohashs),len(addtangs)

		#newdata = get_seg_splits(newdata)	
		#geohashs = np.unique(newdata['GEOHASH']).tolist()
		#tempids = [str(row[position])] * len(geohashs)
		#totalgeohashs += geohashs
		#totalids += tempids
		geohashlist += addgeohashs
		#neighborslist += addneighbors
		#distances += add_distances
		#neighdists += add_neigh
		# printing progress
		if count == 1000:
			total += count
			count = 0
			print '[%s / %s]' % (total,len(data))

		count += 1

	geohashs = [make_sum(i) for i in geohashlist]
	geohashs = '|'.join(geohashs)
	geohashs = [str.split(i,',') for i in str.split(geohashs,'|')]

	data = pd.DataFrame(geohashs,columns=['NEI','OG'])
	data['BOOL'] = data['NEI'].isin(np.unique(data['OG']).tolist())
	data = data[data['BOOL'] == False]

	distancedf = pd.DataFrame(geohashlist,columns=['GEOHASH','DISTANCE','LINEID'])
	distancedfdict = distancedf.set_index('GEOHASH')[['DISTANCE','LINEID']].to_dict()
	data['DISTANCE'] = data['OG'].map(lambda x:distancedfdict['DISTANCE'][x])
	data['LINEID'] = data['OG'].map(lambda x:distancedfdict['LINEID'][x])
	
	data = data[['NEI','DISTANCE','LINEID']]
	data.columns = ['GEOHASH','DISTANCE','LINEID']

	#distancendf = pd.DataFrame(neighdists,columns=['GEOHASH','DISTANCE'])
	
	distancedf = pd.concat([data,distancedf])
	distancedf['MAXDIST'] = distancedf['LINEID'].map(lambda x:segdistance.get(linemask2.get(x,''),''))
	distancedf['PERCENT'] = (distancedf['DISTANCE'] / distancedf['MAXDIST']) * 100
	distancedf['values'] = distancedf[['LINEID','DISTANCE','PERCENT']].values.tolist()	
	distancedf = distancedf.set_index('GEOHASH')['values'].to_dict()
	ultindex = distancedf
	print ultindex.items()[:2]
	'''
	import mapkit as mk

	distancedf['PERCENT'] = (distancedf['DISTANCE'] / distancedf['DISTANCE'].max()) * 100
	distancedf = mk.make_object_map(distancedf,'PERCENT')
	distancedf['COLORKEY'] = distancedf['COLORKEY'].astype(str)
	mk.make_blocks(distancedf,'blocks.geojson',mask=True)
	mk.b()
	'''



	# make line index metadata
	metadata = make_meta_lines(min,max,len(segdistance))

	d = {'ultindex':ultindex,
		'alignmentdf':data1,
		'areamask':linemask2,
		'metadata':metadata}

	dd.io.save(outfilename,d)

	print 'Made output h5 file containing datastructures:'
	print '\t- alignmentdf (type: pd.DataFrame)'
	print '\t- areamask (type: dict)'
	print '\t- ultindex (type: dict)'
	print '\t- metadata (type: dict)'


# makes a line index test block similiar to make
# testblock from polygon_index
def make_line_test(ultindex,number):
	from pipegeohash import random_points_extrema
	extrema = {'n':34.0841,'s':34.069241,'e':-118.25172,'w':-118.22172}
	data = random_points_extrema(number,extrema)
	data = map_table(data,12,map_only=True)
	data = line_index(data,ultindex)
	return data

def applyfunc(array):
	return len(np.unique(array))


def one_line_index(ghash):
	global ultindex
	global areamask
	size = 8
	ind = 0 
	while ind == 0:
		current = ultindex.get(ghash[:size],'')

		if current == '' and size == 9:
			#print ultindex.get(ghash[:8]+'_u','')
			return areamask.get(ultindex.get(ghash[:8]+'_u',''),'')

		elif current == 'na':
			size += 1
		elif current == '':
			return ''
		else:
			return areamask.get(current,'')
def printme(d):
	print d
def return_id_distance(x):
	a = areamask.get(ultindex['LINEID'].get(x[:9],''),'')
	b = ultindex['DISTANCE'].get(x[:9],'')
	return a,b
# maps the points and distances about a gien
# ultindex output (i.e. adds distance and lineid columns)
# this is the mainline usage function for this module
def line_index(data,index):
	global ultindex
	global areamask
	ultindex = index['ultindex']
	areamask = index['areamask']
	# the following apply method retrieves 
	# lineids and distances if available 
	# then is wrapped to retrieve the value in areamask
	# for the lineid values
	holder = data['GEOHASH'].str[:9].map(lambda x:ultindex.get(x,['','','']))
	holder = pd.DataFrame(holder.values.tolist(),columns=['LINEID','DISTANCE','PERCENT'])
	print holder
	data[['LINEID','DISTANCE','PERCENT']] = holder[['LINEID','DISTANCE','PERCENT']]
	data['LINEID'] = data['LINEID'].map(lambda x:areamask.get(x,''))
	
	#data = data.GEOHASH.str[:9].apply(printme)
	#data['LINEID'],data['DISTANCE'] = zip(*data['GEOHASH'].map(return_id_distance))
	#print data.GEOHASH.str[:9].apply(lambda s: pd.Series({'LINEID':areamask.get(ultindex['LINEID'].get(s,''),''), 'DISTANCE':ultindex['DISTANCE'].get(s,'')}))

	#data['LINEID'] = data['GEOHASH'].map(lambda x:areamask.get(ultindex.get(x[:9],''),''))
	return data

# creates metadata dictionary for polygon h5 outputs
# type is the output type
# min and max is the size of polygon steps
# size is the size of areamask
def make_meta_lines(min,max,size):
	return {'type':'lines','minsize':min,'maxsize':max,'size':size}



# for a set of a cordinates gets the max distance
def get_max_distance(coords):
	count = 0
	totaldistance = 0.
	for point in coords:
		if count == 0:
			count = 1
		else:
			dist = distance(oldpoint,point)
			totaldistance += dist
		oldpoint = point
	return totaldistance

# gets a max distance table that will either
# be added to a db or made into a dictionary
def make_max_distance(data):
	from nlgeojson import get_coordstring
	cordbool = False
	for row in data.columns.values.tolist():
		if 'coords' == row:
			cordbool = True
	if cordbool == True:
		cordheader = 'coords'
	else:
		cordheader = 'st_asekwt'
	newlist = []
	for gid,coords in data[['gid',cordheader]].values.tolist():
		if cordbool == True:
			coords = get_cords_json(coords)		
		else:
			coords = get_coordstring(coords)
		maxdistance = get_max_distance(coords)
		newlist.append([gid,maxdistance])

	return pd.DataFrame(newlist,columns=['gid','MAXDISTANCE'])

def add_columns_dbname(dbname,columns,indexcol='gid'):
	string = "dbname=%s user=postgres password=secret" % (dbname)
	conn = psycopg2.connect(string)
	cursor = conn.cursor()
	stringbools = []
	count = 0
	for column in columns:
		if count == 0:
			count = 1
			query = "alter table %s add column %s text" % (dbname,column)	
			stringbools.append(True)
		else:
			query = "alter table %s add column %s float" % (dbname,column)	
			stringbools.append(False)
		try:
			cursor.execute(query)
		except:
			conn = psycopg2.connect(string)
			cursor = conn.cursor()
	conn.commit()

def gener(list):
	for row in list:
		yield row

# function for creating every unique key possible
# also creates a '_u' dictionary and returns both
# this will be used in later functions
def get_allkeys_udict(index):	
	# getting keys
	keys = index['ultindex'].keys()

	# turnning keys into dataframe
	keydf = pd.DataFrame(keys,columns=['KEYS'])
	
	# getting the unique df
	udf = keydf[keydf['KEYS'].str[-2:] == '_u']
	uniq = np.unique(udf['KEYS'].str[:-2]).tolist()
	udf['LINEID'] = udf['KEYS'].map(lambda x:index['ultindex'][x])
	udf = udf.set_index('KEYS')['LINEID'].to_dict()




	# getting all uniques for key df
	uniques = np.unique(keydf['KEYS'].str[:8]).tolist()

	index = []
	ind = 0
	generu = gener(uniques)
	stringlist = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
	stringlist = stringlist * len(uniques)
	while ind == 0:
		try:
			newlist = next(generu)
			newlist = [newlist] * 32
			index += newlist
		except:
			ind = 1

	index = pd.DataFrame(zip(index,stringlist),columns=['KEY','LETTER'])
	index['KEY'] = index['KEY'] + index['LETTER'].astype(str)
	return index['KEY'],udf,keydf,uniq


def get_geohashs_fromindex(index):
	from mapkit import unique_groupby
	# getting geohashs uniquedf and keydf
	geohashs,udf,keydf,uniq = get_allkeys_udict(index)
	geohashs = pd.DataFrame(geohashs.values.tolist(),columns=['GEOHASH'])

	# getting area mask
	areamask = index['areamask']
	# selecting only the size 9 geohashs from keydf
	keys = keydf[keydf['KEYS'].astype(str).str.len() == 9]['KEYS'].values.tolist()
	
	geohashs['BOOL'] = geohashs['GEOHASH'].str[:8].isin(uniq)
	geohashs = geohashs[geohashs.BOOL == True]

	geohashs['BOOL'] = geohashs['GEOHASH'].astype(str).isin(keys)
	geohashs = geohashs[geohashs['BOOL'] == False] 
	geohashs['LINEID'] = geohashs['GEOHASH'].map(lambda x:areamask[udf[x[:8]+'_u']])

	data = pd.DataFrame(index['ultindex'].items(),columns=['GEOHASH','LINEID'])
	data = data[(data['GEOHASH'].str[-2:] != '_u')&(data['LINEID'].astype(str) != 'na')]
	data['LINEID'] = data['LINEID'].map(lambda x:areamask[str(x)])
	data = pd.concat([data,geohashs[['GEOHASH','LINEID']]])
	data = unique_groupby(data,'LINEID',hashfield=True,small=True)	
	dictcolor = data.groupby(['COLORKEY','LINEID']).first()
	dictcolor = dictcolor.reset_index()
	dictcolor = dictcolor.set_index('LINEID')['COLORKEY'].to_dict()
	return data,dictcolor

# given point data and an index returns 
# the blocks dataframe
# the points dataframe
# and the lines dataframe
# for the given set of data and given ultindex h5 output
# returns the configuration for output to pipegls
def make_all_types_lines(pointdata,index,noblocks=False,nopointdata=False):
	from mapkit import make_color_mask,unique_groupby
	hashbool = False
	linebool = False
	if not noblocks == True:
		values = [[i[0],index['areamask'][i[1][0]]] for i in index['ultindex'].items()]
		blocks = pd.DataFrame(values,columns=['GEOHASH','LINEID'])
		blocks = make_color_mask(pointdata,blocks,'LINEID','LINEID')
		#dictcolor = blocks.groupby(['COLORKEY','LINEID']).first().reset_index()
		#dictcolor = blocks.set_index('LINEID')['COLORKEY'].to_dict()	
		#blocks,dictcolor = get_geohashs_fromindex(index)
	if nopointdata == False:
		for row in pointdata.columns.values.tolist():
			if row == 'LINEID':
				linebool = True
			if row == 'GEOHASH':
				hashbool = True
		if hashbool == False:
			pointdata = map_table(pointdata,12,map_only=True)
			hashbool = True
		if hashbool == True and linebool == False:
			pointdata = line_index(pointdata,index)

		# adding colorkeys to point data
		#pointdata = pointdata[pointdata.LINEID.astype(str).str.len() > 0]
		#pointdata['COLORKEY'] = pointdata['LINEID'].map(lambda x:colordict[x])
	
	# creating lines and adding colorkeys
	lines = index['alignmentdf']
	if nopointdata == True:
		lines = make_color_mask(blocks,lines,'LINEID','gid')
	else:
		lines = make_color_mask(pointdata,lines,'LINEID','gid')

	return make_all_configs(pointdata,lines,blocks,noblocks=noblocks,nopointdata=nopointdata)

# makes all the configuration files for a given set of data structures
def make_all_configs(points,lines,blocks,noblocks=False,nopointdata=False):
	from pipegls import make_config
	a = []
	a = make_config(points,'points')
	a = make_config(lines,'lines',current=a)
	a = make_config(blocks,'blocks',current=a)
	return a


################################################################################

# gets the values for geohash1 size 
# that correspond to multiple lines within an upper level block
def get_multiple_uniques(data):
	grouped = data.groupby('GEOHASH1').AREA.nunique()
	grouped = grouped.reset_index()
	grouped.columns = ['GEOHASH1','COUNT']
	grouped = grouped[grouped.COUNT > 1]
	return grouped['GEOHASH1']

# applies the is in statement on data
# and then returns 2 dfs
def separate_data(data,multipleuniques):
	data['BOOL'] = data['GEOHASH1'].isin(multipleuniques)
	
	# getting the multiple df
	multipledf = data[data['BOOL'] == True]

	# getting the single df
	singledf = data[data['BOOL'] == False]

	return singledf,multipledf

# given the multipledf returns a list of all
# returns the list containing the dominant ['geohash1','area'] pairs
def get_dominant_unique(multipledf):
	countdf = multipledf[['GEOHASH','GEOHASH1','AREA']].groupby(['GEOHASH1','AREA']).count()
	countdf = countdf.reset_index()
	countdf.columns = ['GEOHASH1','LINEID','COUNT']
	countdf = countdf.sort(['LINEID','COUNT'],ascending=[1,0])
	countdf = countdf.groupby('GEOHASH1').first().reset_index()
	countdf['TEXT'] = countdf['GEOHASH1'] + ',' + countdf['LINEID']
	return countdf['TEXT']

# this function takes data and the dominant field generated
# and doesan isin statement returning two dfs
# one containg a df that will be made into '_u':dominant
# the other that will be made into the level 9 lowest level dict
def separate_dominant(multipleuniques,dominantfield):
	# creating the text field taht will be compared against the dominant
	# field array
	multipleuniques['TEXT'] = multipleuniques['GEOHASH1'] + ',' + multipleuniques['AREA']

	# creating the bool via the isin statement
	multipleuniques['BOOL'] = multipleuniques['TEXT'].isin(dominantfield)

	# getting the dominant df
	dominantdf = multipleuniques[multipleuniques['BOOL'] == True]

	# getting the single df
	applieddf = multipleuniques[multipleuniques['BOOL'] == False]

	return dominantdf,applieddf

# creates the dictionary for dominant values
# from the dominantdf adding a '_u' to the geohash1
def create_dominant_dict(dominantdf):
	dominantdf['GEOHASH1'] = dominantdf['GEOHASH1'] + '_u'
	return dominantdf.set_index('GEOHASH1')['AREA'].to_dict()

# creates the level 8 mask:'na' dictionary from 
# from the multipledf file (NOT THE SPLIT)
# because all values in the multipledf need a mask
def create_multiple_namask(multipledf):
	uniquemultiples = np.unique(multipledf['GEOHASH1']).tolist()
	return dict(zip(uniquemultiples,len(uniquemultiples)*['na']))

# creating the single dictionary
def create_single_dict(singledf):
	return singledf.set_index('GEOHASH1')['AREA'].to_dict()


def make_neighbors_map(mult):
	area,ghashint = str.split(mult,',')
	ghashs = geohash.neighbors(ghashint)
	ghashs = ['%s,%s' % (ghash,area) for ghash in ghashs if ghash[:8] == ghashint[:8]]
	return '|'.join(ghashs)


# this operation creates the lower line dictionary
# this dictionary returned will be the highest in merge list
# i.e. lowerest priority
def create_applied_neighbors_dict(applydf):
	ghashs = []
	applydf['MULT'] = applydf['AREA'] + ',' + applydf['GEOHASH']
	s = time.time()
	applydf['GEOHASHS'] = applydf['MULT'].map(make_neighbors_map)
	print time.time() - s,'first'
	s = time.time()	
	#holder = applydf.groupby(['AREA','GEOHASH1'])['GEOHASHS'].apply(lambda x: "%s" % '|'.join(x))
	#holder = holder.reset_index()
	applydf['A'] = 'a'
	holder = applydf.groupby('A')['GEOHASHS'].apply(lambda x: "%s" % '|'.join(x))
	holder = holder.loc['a']
	del applydf
	holder = [str.split(i,',') for i in str.split(holder,'|')]
	#holder = pd.DataFrame(holder,columns = ['TEXT'])
	#holder2 = holder['TEXT'].str.split(',',expand=True)
	print time.time() - s,'second'
	holder = pd.DataFrame(holder,columns=['GEOHASH','AREA'])
	return holder.set_index('GEOHASH')['AREA'].to_dict()


def create_ultindex(data):
	# creating geohash1 size on df
	data['GEOHASH1'] = data['GEOHASH'].str[:8]

	# selecting the geohash1s with multiple lines within them
	multipleuniques = get_multiple_uniques(data)

	# creating multipledf and singledf 
	singledf,multipledf = separate_data(data,multipleuniques)

	# single df contains or needs just a flat dictionary
	# while multiple df needs
	# {'g8':'na'},{'g8_u':dominantunique},{level 9 alone},{level 9 neighbors}
	# make sure level 9 alone is last
	dominantuniques = get_dominant_unique(multipledf)

	# separating dominant
	# the dominant df is '_u':dominantid
	# the applied is level 9 on alignments and neighbors
	dominantdf,applydf = separate_dominant(multipledf,dominantuniques)

	# creating the nadict dictionary
	nadict = create_multiple_namask(multipledf)

	# creating the dominant dict 
	dominantdict = create_dominant_dict(dominantdf)

	# creating the single dict
	singledict = create_single_dict(singledf)

	# creating applieddictneighbors
	appliedneighdict = create_applied_neighbors_dict(applydf)

	# creating the applydf by itself
	applydict = applydf.set_index('GEOHASH')['AREA'].to_dict()

	# merging dictionaries and creating ultindex
	ultindex = merge_dicts(*[nadict,dominantdict,singledict,appliedneighdict,applydict])

	return ultindex