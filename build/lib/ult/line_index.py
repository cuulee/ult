import pandas as pd
import numpy as np
import itertools
import geohash
import time
import json
from pipegeohash import map_table,list2df,df2list
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
def generate_points_geohash(number_of_points,point1,point2,name,size,currentdist,maxdistance):
	# getting x points
	geohashlist = []

	x1,x2 = point1[0],point2[0]
	xdelta = (float(x2) - float(x1)) / float(number_of_points)
	xcurrent = x1

	# getting y points
	y1,y2 = point1[1],point2[1]
	ydelta = (float(y2) - float(y1)) / float(number_of_points)
	ycurrent = y1
	g1 = geohash.encode(y1,x1,size)
	geohashlist = ['GEOHASH',geohash.encode(y1,x1,size)]
	pointdelta = (xdelta ** 2 + ydelta ** 2) ** .5
	current = currentdist
	stringlist = ['GEOHASH','%s,%s,%s,%s' % (g1,name,str(currentdist),str(maxdistance))]
	count = 0
	strmaxdistance = str(maxdistance)
	while count < number_of_points:
		count += 1
		xcurrent += xdelta
		ycurrent += ydelta
		current += pointdelta
		ghash = geohash.encode(ycurrent,xcurrent,size)
		geohashlist.append(ghash)
		stringlist.append('%s,%s,%s,%s' % (ghash,name,str(current),strmaxdistance))
	geohashlist.append(geohash.encode(point2[1],point2[0],size))
	lastdist = currentdist + distance(point1,point2)
	g2 = geohash.encode(y2,x2,size)

	stringlist.append('%s,%s,%s,%s' % (g2,name,str(lastdist),strmaxdistance))
	
	indexs = np.unique(geohashlist,return_index=True)[1]
	stringlist = [stringlist[i] for i in sorted(indexs)]
	return stringlist

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
def fill_geohashs(data,name,size,maxdistance):
	global ghashdict
	# function for linting whether the first point and lastpoint are the same if not appends talbe
	hashsize = get_hashsize(data[0],size)

	count = 0
	geohashlist = []
	tangslist = []
	currentdist = 0.
	neighbors = []
	ghashdict = {}
	dist = 0
	ind = 0
	for row in data:
		if count == 0:
			count = 1
			geohashlist.append('%s,%s,%s,%s' % (geohash.encode(row[0],row[1],9),name,str(currentdist),str(maxdistance)))
		else:
			slope = get_slope(oldrow,row)
			x1,y1 = oldrow
			dist = distance(oldrow,row)
			positions = solve_xmin(oldrow,row,size)

			if dist > hashsize / 5.0 or ind == 0:
				number = (dist / hashsize) * 5.0
				number = int(number)				

				if ind == 0 and not dist == 0 and not number > 10:
					ind = 1
					number = 10

				addghashs = generate_points_geohash(number,oldrow,row,name,size,currentdist,maxdistance)[1:]

				geohashlist += addghashs
			else:
				point = row
				
				geohashlist.append('%s,%s,%s,%s' % (geohash.encode(point[0],point[1],9),name,str(currentdist),str(maxdistance)))

			currentdist += dist

		oldrow = row

	return geohashlist


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
		key = i
		linemask2[key] = unique
		linemask1[unique] = key

	return linemask1,linemask2

# encoding geeohash neighborsas as a string
def map_geohash_neighbors(ghash):
	return '|'.join(['%s,%s' % (ghash,i) for i in geohash.neighbors(ghash)])


# creates metadata dictionary for polygon h5 outputs
# type is the output type
# min and max is the size of polygon steps
# size is the size of areamask
def make_meta_lines(min,max,size):
	return {'type':'lines','minsize':min,'maxsize':max,'size':size,'output_type':'single'}


# making line segment index
def make_line_index(data,outfilename,**kwargs):
	data1 = data
	uniqueid = False
	filename = False
	return_index = False
	precision = 9
	benchmark = False
	for key,value in kwargs.iteritems():
		if key == 'uniqueid':
			uniqueid = value
		if key == 'precision':
			precision = value
		if key == 'benchmark':
			benchmark = value


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


 	starttime = time.time()
 	linemask1,linemask2 = make_line_mask(data)

 	count = 0
 	geohashlist = []
 	neighbors = []

 	# collecing all the alignment geohashs
 	for coords,row in itertools.izip(data['coords'].map(get_cords_json).values.tolist(),data.values.tolist()):
		name = row[uniqueidrow]
		maxdistance = segdistance[int(name)]
		
		addgeohashs = fill_geohashs(coords,name,precision,maxdistance)
		geohashlist += addgeohashs
		if count == 1000:
			total += count
			count = 0
			print '[%s / %s]' % (total,len(data))

		count += 1

	endloop = time.time() - starttime

	startdftime = time.time()
	s = time.time()
	count = 0
	aligndicts = []
	dict2s = []

	# construction of the neighboring goehashs
	# iterating through each secton of the large dataframe were splitting up
	for aligns in np.array_split(pd.DataFrame(geohashlist,columns=['TEXT']),20):
		count+= 1
		print '[%s / %s]' % (count,20)

		# getting geohshs
		aligns['GEOHASH'] = aligns['TEXT'].str[:9]
	
		#aligns['value'] = aligns[['LINEID','DISTANCE','PERCENT']].values.tolist()
		aligns['NEIGHBORS'] = aligns['GEOHASH'].map(map_geohash_neighbors)
		
		# aggregating text
		aligns['a'] = 'a'
		totalneighbors = aligns.groupby('a')['NEIGHBORS'].apply(lambda x: "%s" % '|'.join(x))['a']
		totalneighbors = pd.DataFrame([i for i in str.split(totalneighbors,'|')],columns=['TEXT'])

		# slicing the text field to get the appropriate geohashs
		totalneighbors['INNERGEOHASH'],totalneighbors['NEIGHBORS'] = totalneighbors['TEXT'].str[:9],totalneighbors['TEXT'].str[10:]

		# gettin gthe aligns dict
		alignsdict = aligns.set_index('GEOHASH')['TEXT'].str[10:].to_dict()

		# mapping the text to the neighboriing geohashs
		totalneighbors['TEXT'] = totalneighbors['INNERGEOHASH'].map(lambda x:alignsdict[x])
		dict2 = totalneighbors.set_index('NEIGHBORS')['TEXT'].to_dict()
		aligndicts.append(alignsdict)
		dict2s.append(dict2)
	

	# mering both dictionaries.
	ultindex = merge_dicts(*dict2s+aligndicts)


	enddftime = time.time() - startdftime


	# make line index metadata
	metadata = make_meta_lines(8,9,len(segdistance))
	df = pd.DataFrame([json.dumps(ultindex),json.dumps(linemask2),json.dumps(metadata)],index=['ultindex','areamask','metadata'])
	# writing output to h5 file
	if benchmark == False:
		with pd.HDFStore(outfilename) as out:
			out['combined'] = df
			out['alignmentdf'] = data

	print 'Made output h5 file containing datastructures:'
	print '\t- alignmentdf (type: pd.DataFrame)'
	print '\t- areamask (type: dict)'
	print '\t- ultindex (type: dict)'
	print '\t- metadata (type: dict)'


	totaltime = time.time() - starttime

	return pd.DataFrame([['looptime',endloop],
						['df time',enddftime],
						['totaltime',totaltime]],
						columns = ['FIELD','TIME'])

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
		cordheader = 'st_asewkt'
	newlist = []
	for gid,coords in data[['gid',cordheader]].values.tolist():
		if cordbool == True:
			coords = get_cords_json(coords)		
		else:
			coords = get_coordstring(coords)
		maxdistance = get_max_distance(coords)
		newlist.append([gid,maxdistance])

	return pd.DataFrame(newlist,columns=['gid','MAXDISTANCE'])



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
	data['TEXT'] = data['GEOHASH'].str[:9].map(lambda x:ultindex.get(x,''))
	return data

# makes a line index test block similiar to make
# testblock from polygon_index
def make_line_test(ultindex,number):
	from pipegeohash import random_points_extrema
	extrema = {'n':36.0841,'s':33.069241,'e':-120.25172,'w':-117.22172}
	data = random_points_extrema(number,extrema)
	data = map_table(data,12,map_only=True)
	data = line_index(data,ultindex)
	data = data[data['TEXT'].astype(str).str.len() > 0]
	return data

def applyfunc(array):
	return len(np.unique(array))

# the (old) mapped function
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

# splits the output returned from line_index
def split_text_output(df):
	from mapkit import make_colorkey
	df[['LINEID','DISTANCE','MAXDIST']] = df['TEXT'].astype(str).str.split(',',expand=True).astype(float)
	df['PERCENT'] = (df['DISTANCE'] / df['MAXDIST'])	* 100.
	return make_colorkey(df,'PERCENT',numeric=True,linear=True)


def df_from_ultindex(ultindex):
	return pd.DataFrame(ultindex['ultindex'].items(),
		columns = ['GEOHASH','TEXT'])
def index_df(ultindex):
	df = df_from_ultindex(ultindex)
	return split_text_output(df)

def return_id_distance(x):
	a = areamask.get(ultindex['LINEID'].get(x[:9],''),'')
	b = ultindex['DISTANCE'].get(x[:9],'')
	return a,b



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