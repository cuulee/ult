import pandas as pd
import numpy as np
import itertools
import geohash
import time
import json
from pipegeohash import map_table,list2df,df2list

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

	geohashlist = ['GEOHASH',geohash.encode(point1[1],point1[0],size)]

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



# hopefully a function can be made to properly make into lines
def fill_geohashs(data,size):
	# function for linting whether the first point and lastpoint are the same if not appends talbe

	
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

	count = 0
	geohashlist = []
	for row in data:
		if count == 0:
			count = 1
		else:
			dist = distance(oldrow[:-1],row[:-1])
			if dist > hashsize / 5.0:
				number = (dist / hashsize) * 5.0
				number = int(number)
				geohashlist += generate_points_geohash(number,oldrow[:-1],row[:-1],row[2],size)[1:]
			else:
				point = row[:-1]
				geohashlist.append(geohash.encode(point[1],point[0],size))
		oldrow = row
	a = geohashlist
	indexes = np.unique(a, return_index=True)[1]
	geohashlist = [a[index] for index in sorted(indexes)]
	newlist = zip(geohashlist,[row[2]] * len(geohashlist))
	return newlist


# making flat list non sorted
def flatten_nonsorted(data):
	ghashs = data['GEOHASH']
	indexes = np.unique(ghashs, return_index=True)[1]
	list = [ghashs[index] for index in sorted(indexes)]
	return list	

# making line segment index
def make_line_index(data,outfilename,**kwargs):
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

 	count = 0
 	newlist = []
 	for coords,row in itertools.izip(data['coords'].map(get_cords_json).values.tolist(),data.values.tolist()):
		L1 = coords
		L2 = [[row[uniqueidrow]]] * len(coords)
		coords = [x + y for x,y in zip(L1,L2)]
		
		#newdata = pd.DataFrame(row,columns=['LONG','LAT'])

		newdata = fill_geohashs(coords,precision)

		#newdata = get_seg_splits(newdata)	
		#geohashs = np.unique(newdata['GEOHASH']).tolist()
		#tempids = [str(row[position])] * len(geohashs)
		#totalgeohashs += geohashs
		#totalids += tempids
		newlist += newdata
		# printing progress
		if count == 1000:
			total += count
			count = 0
			print '[%s / %s]' % (total,len(data))

		count += 1

	# creating the dataframe that will be used for the index
	totaldf = pd.DataFrame(newlist,columns=['GEOHASH','AREA'])

	# sending total df into make_linejson
	make_line_json(totaldf,segdistance,outfilename)


	#totaldf = totaldf[['GEOHASH','AREA']].groupby('GEOHASH').first()
	if return_index == True:
		return totaldf
	else:
		return totaldf

	return totaldf

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
	size = 8
	ind = 0 
	while ind == 0:
		current = ultindex.get(ghash[:size],'')

		if current == '' and size == 9:
			#print ultindex.get(ghash[:8]+'_u','')
			return ultindex.get(ghash[:8]+'_u','')

		elif current == 'na':
			size += 1
		elif current == '':
			return ''
		else:
			return current


def line_index(data,index):
	global ultindex
	ultindex = index

	data['LINEID'] = data['GEOHASH'].map(one_line_index)
	return data


def make_line_json(data,segdistance,outfilename):
	olddata = data
	data['GEOHASH1'] = data['GEOHASH'].str[:-1]

	df = data[['GEOHASH1','AREA']].groupby(['GEOHASH1']).first()
	grouped = data[['GEOHASH1','AREA']].groupby(['GEOHASH1'])
	df['NO_SEGS'] = grouped['AREA'].apply(applyfunc)
	df1 = df[df['NO_SEGS'] > 1]
	df2 = df[df['NO_SEGS'] == 1]
	df2 = df2.reset_index()[['GEOHASH1','AREA']]
	df2.columns = ['GEOHASH','AREA']

	newdata = df.loc[df1.index]
	newdata = newdata.reset_index()
	newdata.to_csv('solves.csv',index=False)
	uniques =  np.unique(newdata['GEOHASH1']).tolist()



	data['BOOL'] = olddata['GEOHASH'].str[:-1].isin(uniques)
	data = data[data['BOOL'] == True]
	data = data[['GEOHASH','AREA']]
	data1 = df2
	data['GEOHASH1'] = data['GEOHASH'].str[:8]
	uniques = np.unique(data['GEOHASH1']).tolist()
	outerdict = dict(zip(uniques,['na'] * len(uniques)))

	grouplist = []
	groupnames = []
	count = 0
	for name,group in data.groupby('GEOHASH1'):
		areas = np.unique(group['AREA']).tolist()
		maxdistance = 0
		for area in areas:
			if segdistance[area] > maxdistance:
				maxdistance = segdistance[area]
				maxarea = area
		
		# getting new areas not part of max area
		newareas = []
		for area in areas:
			if not area == maxarea:
				newareas.append(area)

		groupname = [name+'_u',maxarea]

		#group = group.set_index('GEOHASH')
		#groupdict = group.to_dict()['AREA']
		#groupdict[name+'_u'] = maxarea
		#print groupdict.keys()
		grouplist += newareas
		groupnames.append(groupname)

	data['BOOL'] = data['GEOHASH'].isin(grouplist)
	data = data[data['BOOL'] == True]
	data = data.set_index('GEOHASH')
	data = data['AREA'].to_dict()


	data1 = data1.set_index('GEOHASH')
	data1 = data1['AREA'].to_dict()


	ultindex = ult.merge_dicts(*[outerdict,data,data1])

	for key,value in groupnames:
		ultindex[key] = value

	ult.make_json(ultindex,outfilename)

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
import mapkit as mk

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
'''

import mapkit as mk
import pandas as pd
import ult
import numpy as np
data = pd.read_csv('a.csv')
ultindex = ult.read_json('nyc.json')


data = ult.map_table(data,12,map_only=True)
data = ult.line_index(data,ultindex)

data = data[data['LINEID'].astype(str).str.len() > 0]
data = mk.unique_groupby(data,'LINEID',small=True,hashfield=True)

lines = mk.select_fromindexs('nyc_streets','gid',np.unique(data['LINEID'].astype(int)).tolist())
lines = mk.make_color_mask(data,lines,'LINEID','gid')

c = mk.make_config(lines,'lines',current=mk.make_config(data,'points'))

mk.eval_config(c)



#mk.cln()
#mk.make_lines(lines,'lines.geojson',mask=True)
#mk.make_points(data,'points.geojson',mask=True)
#mk.a()
# now we need to do this solve-
segdistance = ult.read_json('segdist.json')

'''
'''

ultindex = ult.read_json('new_index2.json')


data = pd.read_csv('total_philly.csv')
data.columns = data.columns.values.tolist()[:-2] + ['LAT','LONG']

data = ult.map_table(data,12,map_only=True)
data = line_index(data,ultindex)

data = data[data['LINEID'].astype(str).str.len() > 0]
data = mk.unique_groupby(data,'LINEID',small=True,hashfield=True)

lines = mk.get_database('philly')

lines = mk.make_color_mask(data,lines,'LINEID','gid')

a = mk.make_config(data,'points')
a = mk.make_config(lines,'lines',current=a)
mk.eval_config(a)

'''

