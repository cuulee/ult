import pandas as pd
import numpy as np
import itertools
import berrl as bl
import geohash
import time

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

# given a point1 x,y and a point2 x,y returns distance in miles
# points are given in long,lat geospatial cordinates
def distance(point1,point2):
	point1 = np.array(point1)
	point2 = np.array(point2)
	return np.linalg.norm(point1-point2)


# extends a new table down from a post gis row and header
def extend_geohashed_table(header,extendrow,precision):
	count=0
	newheader=[]
	newvalues=[]
	for a,b in itertools.izip(header,extendrow):
		if a=='st_asewkt':
			geometrypos=count
		elif a=='geom':
			pass
		else:
			newheader.append(a)
			newvalues.append(b)
		count+=1

	# adding values to newheader that were in the text geometry
	newheader=newheader+['LAT','LONG','DISTANCE','GEOHASH']
	
	# parsing through the text geometry to yield what will be rows
	try: 
		geometry=extendrow[geometrypos]
		geometry=str.split(geometry,'(')
		geometry=geometry[-1]
		geometry=str.split(geometry,')')
		geometry=geometry[:-2][0]
		geometry=str.split(geometry,',')

		# setting up new table that will be returned as a dataframe
		newtable=[newheader]
		firstrow = geometry[0]
		firstrow = str.split(firstrow,' ')
		olddist = float(firstrow[-1])
		ghash = ''
		geohashs = []
		for row in geometry:
			row = str.split(row,' ')
			distance = float(row[-1]) - olddist 
			lat = float(row[1])
			long = float(row[0])
			try: 
				hash = geohash.encode(float(lat), float(long), precision)
				if not hash == ghash:
					ghash = hash
					geohashs.append(ghash)
				newrow = newvalues+[lat,long,distance,hash]
				newtable.append(newrow)
			except Exception:
				pass
			olddist = float(row[-1])
	except Exception:
		newtable=[['GEOHASH'],['']]

	# taking table from list to dataframe
	newtable = bl.list2df(newtable)

	return geohashs,newtable


# for a given line alignment gets the segment split parameters
# hopefully a function can be made to properly make into lines
def get_seg_splits(data):
	extrema = get_extrema(data)

	# getting upper lefft and lowerright point
	ul = [extrema['w'],extrema['n']]
	lr = [extrema['e'],extrema['s']]


	# getting geohash for ul and lr
	# assuming 8 for the time being as abs min
	ulhash = geohash.encode(ul[1],ul[0],8)
	lrhash = geohash.encode(lr[1],lr[0],8)

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
	newlist = bl.map_table(newlist,8,map_only=True)
	return newlist

# making line segment index
def make_line_index(data,headercolumn,**kwargs):
	csv = False
	filename = False
	for key,value in kwargs.iteritems():
		if key == 'csv':
			csv = value
		if key == 'filename':
			filename = value

	if filename == False:
		filename = 'line_index.csv'


	# getting header and intializing
	header = data.columns.values.tolist()
	totalgeohashs = []
	totalids = []
 	count = 0
 	total = 0
 	
 	# getting unique column position
 	for row in header:
 		if headercolumn in str(row):
 			position = count
 		count += 1

 	count = 0
 	for row in data.values.tolist():
		geohashs,newdata = extend_geohashed_table(header,row,8)
		newdata = get_seg_splits(newdata)	
		geohashs = np.unique(newdata['GEOHASH']).tolist()
		tempids = [str(row[position])] * len(geohashs)
		totalgeohashs += geohashs
		totalids += tempids

		# printing progress
		if count == 1000:
			total += count
			count = 0
			print '[%s / %s]' % (total,len(data))

		count += 1

	# creating the dataframe that will be used for the index
	totaldf = pd.DataFrame(totalgeohashs,columns=['GEOHASH'])
	totaldf[str(headercolumn)] = totalids

	if csv == True:
		totaldf.to_csv(filename,index=False)

	return totaldf

# create dictionary object from dataframe object
def make_lineindex_dict(df):
	segdict = {}
	for row in df.values.tolist():
		segdict[str(row[0])] = str(row[1])
	return segdict

# mapping each point to dictionary index
def map_level_one(ghash):
	global linedict
	try:
		return linedict[str(ghash)]
	except KeyError:
		return ''


# encapsulating function for georeferencing a point on a line segement
def map_lineindex(data,index,headercolumn,maxsize):
	global linedict
	linedict = index
	data[headercolumn] = data['GEOHASH'].str[:maxsize].map(map_level_one)
	return data

# creating a new index that will specifically contain
# geohashs that contain multiple alignments
# this may be used for a hiearchical correction against
# speed, last routeid, passed a certain geohash along a route, i.e. a junction
def make_multi_dict(data,headerval):
	collision_dict = {}
	for row in data.values.tolist():
		try:
			current = collision_dict[str(row[0])]
		except KeyError:
			current = []
		current.append(row[1])
		collision_dict[str(row[0])] = current
	
	collision_dict2 = {}
	for row in collision_dict.keys():
		testval = collision_dict[str(row)]
		if not len(testval) == 1:
			collision_dict2[str(row)] = testval		

	return collision_dict2

'''
bl.clean_current()
data2 = pd.read_csv('la_routes.csv')

data = pd.read_csv('la_index.csv')
multi_dict = make_multi_dict(data,'gid')
geohashs = multi_dict.keys()
b = bl.make_geohash_blocks(geohashs[:40000])


newlist = []
for row in geohashs[:40000]:
	vals = multi_dict[row]
	newvals = []
	for row in vals:
		newvals.append(str(row))
	newlist += newvals
gids = np.unique(newlist)
print len(gids)
data2['BOOL'] = data2['gid'].astype(str).isin(gids)
data2 = data2[data2['BOOL'] == True]


bl.make_blocks(b,f='',bounds=True)

bl.make_postgis_lines(data2,filename='lines.geojson',bounds=True)
bl.a(bounds=True)
#data2 = pd.read_csv('la_routes.csv')
#line_df = make_line_index(data,'gid',csv=True,filename='la_index.csv')
'''
'''
line_dictionary = make_lineindex_dict(data)

bl.clean_current()

extrema = {'n': 34.707659,'s': 33.246960,'w':-119.067929,'e':-116.991248}
sampledata = bl.random_points_extrema(10000,extrema)

# mapping geohashs to each point
sampledata = bl.map_table(sampledata,8,map_only=True)



s = time.time()
sampledata = map_lineindex(sampledata,line_dictionary,'gid',8)
print (time.time() - s) / float(len(sampledata))

sampledata = sampledata[sampledata['gid'].str.len() > 0]
uniquegids = np.unique(sampledata['gid'].astype(str)).tolist()

data2['BOOL'] = data2['gid'].astype(str).isin(uniquegids)
data2 = data2[data2['BOOL'] == True] 


print 'done'

bl.make_postgis_lines(data2,'lines.geojson')
bl.make_points(sampledata,filename='points.geojson')
bl.a()


'''