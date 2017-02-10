import future
import pandas as pd
import numpy as np
import geohash
import simplejson as json
from geohash_logic import get_slope,solve_xmin
import os
import time
from multiprocessing import Process
import random


# given a point1 x,y and a point2 x,y returns distance in miles
# points are given in long,lat geospatial cordinates
def distance(point1,point2):
	point1 = np.array(point1)
	point2 = np.array(point2)
	return np.linalg.norm(point1-point2)

def get_cords_json(coords):
	data = '{"a":%s}' % coords
	data = json.loads(data)	
	return data['a']

def neighbors_func(ghash):
	nei = geohash.neighbors(ghash)
	return '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (ghash,nei[0],ghash,nei[1],ghash,nei[2],ghash,nei[3],ghash,nei[4],ghash,nei[5],ghash,nei[6],ghash,nei[7])



# returns a set of points that traverse the linear line between two points
def generate_points_geohash(number_of_points,point1,point2,name,size,currentdist,maxdistance):
	# getting x points
	geohashlist = []
	if number_of_points == 0:
		return []

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
	stringlist = [[g1,'%s,%s,%s' % (g1,name,str(currentdist),)]]
	count = 0
	strmaxdistance = str(maxdistance)
	while count < number_of_points:
		count += 1
		xcurrent += xdelta
		ycurrent += ydelta
		current += pointdelta
		ghash = geohash.encode(ycurrent,xcurrent,size)
		geohashlist.append(ghash)
		stringlist.append([ghash,'%s,%s,%s' % (ghash,name,str(current))])
	geohashlist.append(geohash.encode(point2[1],point2[0],size))
	lastdist = currentdist + distance(point1,point2)
	g2 = geohash.encode(y2,x2,size)

	stringlist.append([g2,'%s,%s,%s' % (g2,name,str(lastdist))])
	indexs = np.unique(geohashlist,return_index=True)[1]
	try:
		stringlist = [stringlist[i] for i in sorted(indexs)]
	except:
		return []
	return stringlist


# function for writing a json ot to a file
def make_json(dictionary,filename):
	with open(filename,'wb') as f:
		json.dump(dictionary,f)

# function for reading json 
def read_json(filename):
	with open(filename,'rb') as f:
		return json.load(f)





# hopefully a function can be made to properly make into lines
def fill_geohashs(data,name,size,maxdistance,hashsize):
	global ghashdict
	# function for linting whether the first point and lastpoint are the same if not appends talbe

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
			geohashlist.append([geohash.encode(row[1],row[0],9),'%s,%s,%s' % (geohash.encode(row[1],row[0],9),name,str(currentdist))])
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
				geohashlist.append([geohash.encode(point[1],point[0],9),'%s,%s,%s' % (geohash.encode(point[1],point[0],9),name,str(currentdist))])

			currentdist += dist

		oldrow = row

	return geohashlist



# creates metadata dictionary for polygon h5 outputs
# type is the output type
# min and max is the size of polygon steps
# size is the size of areamask
def make_meta_lines(min,max,size,extrema):
	return {'type':'lines','minsize':min,'maxsize':max,'size':size,'output_type':'single','extrema':extrema}

# makes a line mask for the lineids of a given df
def make_line_mask(data):
	linemask1 = {}
	linemask2 = {}
	uniques = np.unique(data['gid']).tolist()
	for i,unique in zip(range(len(uniques)),uniques):
		key = i
		linemask2[key] = unique
		linemask1[unique] = key

	return linemask1,linemask2

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

def merge_dicts(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    '''
    result = {}
    size = len(dict_args)
    count = 0
    for dictionary in dict_args:
        result.update(dictionary)
        count += 1
        print("[%s/%s]" % (count,size))
    return result


# function that instantiates the make_line_index abstracton
def helper_func(name,filename,split,appendsize):
	make_line_index(name,filename,multiprocessing=True,split=split,appendsize=appendsize)
	print('Split %s COMPLETE.' % split)

# function for making multiple processes
def make_processes(data,filename,number_of_processes,appendsize):

	# shuffling data to remove uneven split distances sometimes
	data = data.sample(n=len(data))


	extrema = []
	count = 0
	processes = []
	for i in np.array_split(data,number_of_processes):
		count += 1
		p = Process(target=helper_func,args=(i,filename,count,appendsize))
		p.start()
		with open('process%s.csv' % count,'wb') as b:
			b.write(b'"GEOHASH","TEXT"')		
		print 'Started Process %s' % count
		processes.append(p)

	# loop until all processes are done
	ind=True
	while ind == True:
		time.sleep(1)
		runind = True
		for i in processes:
			status = i.is_alive()
			if status == True:
				runind = False
		if runind == True:
			ind = False



	# making lines mask for no reason
	linemask1,linemask2 = make_line_mask(data)

	# make line index metadata
	metadata = make_meta_lines(8,9,len(data),extrema)
	df = pd.DataFrame(['stringdf',json.dumps(linemask2),json.dumps(metadata)],index=['ultindex','areamask','metadata'])

	# writing output to h5 file
	if not filename == False:
		with pd.HDFStore(filename) as out:
			out['combined'] = df
			out['alignmentdf'] = data
			count = 0
			data = []
			for i in processes:
				count += 1
				csvfilename = 'process%s.csv' % count
				data.append(pd.read_csv(csvfilename))
				os.remove(csvfilename)
			out['ultindex'] = pd.concat(data,ignore_index=True)







def make_line_index(data,h5filename,multiprocessing=False,split=1,processes=1,appendsize=5000):
	if not processes == 1:
		multiprocessing = True
		print('Creating multiple processes.')
		make_processes(data,h5filename,processes,appendsize)
		return []

	csvfilename = 'process%s.csv' % split

	try:
		os.remove(h5filename)
	except:
		pass

	gidbool = False
	# checking to see if gid exists
	for row in data.columns:
		if row == 'gid':
			gidbool = True

	coordbool = False
	# checking to see if gid exists
	for row in data.columns:
		if row.lower() == 'coords':
			coordbool = True
			coordheader = row


	# getting maxdistance if applicable
	maxdistance = False
	for i in data.columns:
		if 'maxdistance' in str(i).lower():
			maxdistanceheader = i
			maxdistancebool = True


	# logic for zipping together right obects
	if gidbool == True:
		iterdata = data[['gid',coordheader,maxdistanceheader]].values
	else:
		iterdata = zip(range(len(data)),data[coordheader],data[maxdistanceheader])


	# retriving the sise of 1 geohash
	firstpoint = get_cords_json(data[coordheader].values[0])[0]
	lat,long,latdelta,longdelta = geohash.decode_exactly(geohash.encode(firstpoint[1],firstpoint[0],9))
	ghashsize = ((latdelta*2)**2 + (longdelta*2)**2) ** .5

	# checking for cardinal coords and getting extrema if applicable
	ind = 0
	for row in data.columns:
		if 'north' in str(row).lower():
			nhead = row
			ind += 1
		if 'south' in str(row).lower():
			shead  = row
			ind += 1
		if 'east' in str(row).lower():
			ehead = row
			ind += 1
		if 'west' in str(row).lower():
			whead  = row
			ind += 1

	# logic for getting the extrema
	if ind == 4:
		extrema = { 'n':data[nhead].max(),
		  's':data[shead].min(),
		  'w':data[whead].min(),
		  'e':data[ehead].max()}
	else:
		extrema = []


	size = len(data)
	addgeohashs = []
	total = 0
	count = 0
	msgsize = 0
	jsonfilename = str.split(h5filename,'.')[0] + '.csv'



	for gid,coords,maxdistance in iterdata:
		coords = get_cords_json(coords)
		addgeohashs += fill_geohashs(coords,gid,9,maxdistance,ghashsize)
		count += 1
		if count == appendsize:
			count = 0
			total += appendsize
			aligns = pd.DataFrame(addgeohashs,columns=['GEOHASH','TEXT'])
			

			print('Process %s: [%s/%s] dfsize created: %s' % (split,total,size,len(addgeohashs)))
			

			addgeohashs = []
			msgsize += 1
			#aligns['value'] = aligns[['LINEID','DISTANCE','PERCENT']].values.tolist()
			#aligns['NEIGHBORS'] = aligns['GEOHASH'].map(lambda x:'|'.join(['%s,%s' % (x,i) for i in geohash.neighbors(x)]))
			aligns = aligns.loc[np.unique(aligns['GEOHASH'],return_index=True)[1]]
			#aligns['GEOHASH'] = aligns.index

			# aggregating text
			aligns['a'] = 'a'
			#totalneighbors = aligns.groupby('a')['GEOHASH'].apply(lambda x: '|'.join(['|'.join(['|'.join(['%s,%s' % (ii,i) for i in geohash.neighbors(ii)])]) for ii in x.values]))['a']
			totalneighbors = pd.DataFrame(str.split(aligns.groupby('a')['GEOHASH'].apply(lambda x: '|'.join(['|'.join(['|'.join(['%s,%s' % (ii,i) for i in geohash.neighbors(ii)])]) for ii in x.values]))['a'],'|'),columns=['TEXT'])

			# slicing the text field to get the appropriate geohashs
			totalneighbors['NEIGHBORS'] = totalneighbors['TEXT'].str[10:]

			# doing total neighbors
			totalneighbors = totalneighbors.loc[np.unique(totalneighbors['NEIGHBORS'],return_index=True)[1]]

			# mapping the text to the neighboriing geohashs
			totalneighbors['TEXT'] = aligns.set_index('GEOHASH').loc[totalneighbors['TEXT'].str[:9]]['TEXT'].values
			#totalneighbors = totalneighbors.groupby('NEIGHBORS').first()
			#totalneighbors = totalneighbors['TEXT'].to_json(orient='index')
			#print(totalneighbors[['NEIGHBORS','TEXT']].to_csv(index=False,header=False,mode=str)+'\n'+aligns[['GEOHASH','TEXT']].to_csv(index=False,header=False,mode=str))
			
			aligns['TEXT'] = aligns['TEXT'].str[10:]
			totalneighbors['TEXT'] = totalneighbors['TEXT'].str[10:]
			with open(csvfilename,'a') as f:
				f.write('\n'+totalneighbors[['NEIGHBORS','TEXT']].to_csv(index=False,header=False,mode=str)+'\n'+aligns[['GEOHASH','TEXT']].to_csv(index=False,header=False,mode=str))	



	
	if not count == 0:
		aligns = pd.DataFrame(addgeohashs,columns=['GEOHASH','TEXT'])
		print('Process %s: [%s/%s] dfsize created: %s' % (split,total,size,len(addgeohashs)))
		addgeohashs = []
		msgsize += 1
		#aligns['value'] = aligns[['LINEID','DISTANCE','PERCENT']].values.tolist()
		#aligns['NEIGHBORS'] = aligns['GEOHASH'].map(lambda x:'|'.join(['%s,%s' % (x,i) for i in geohash.neighbors(x)]))
		aligns = aligns.loc[np.unique(aligns['GEOHASH'],return_index=True)[1]]
		#aligns['GEOHASH'] = aligns.index

		# aggregating text
		aligns['a'] = 'a'
		#totalneighbors = aligns.groupby('a')['GEOHASH'].apply(lambda x: '|'.join(['|'.join(['|'.join(['%s,%s' % (ii,i) for i in geohash.neighbors(ii)])]) for ii in x.values]))['a']
		totalneighbors = pd.DataFrame(str.split(aligns.groupby('a')['GEOHASH'].apply(lambda x: '|'.join(['|'.join(['|'.join(['%s,%s' % (ii,i) for i in geohash.neighbors(ii)])]) for ii in x.values]))['a'],'|'),columns=['TEXT'])

		# slicing the text field to get the appropriate geohashs
		totalneighbors['NEIGHBORS'] = totalneighbors['TEXT'].str[10:]

		# doing total neighbors
		totalneighbors = totalneighbors.loc[np.unique(totalneighbors['NEIGHBORS'],return_index=True)[1]]

		# mapping the text to the neighboriing geohashs
		totalneighbors['TEXT'] = aligns.set_index('GEOHASH').loc[totalneighbors['TEXT'].str[:9]]['TEXT'].values
		aligns['TEXT'] = aligns['TEXT'].str[10:]
		totalneighbors['TEXT'] = totalneighbors['TEXT'].str[10:]

		#totalneighbors = totalneighbors.groupby('NEIGHBORS').first()
		#totalneighbors = totalneighbors['TEXT'].to_json(orient='index')
		#print(totalneighbors[['NEIGHBORS','TEXT']].to_csv(index=False,header=False,mode=str)+'\n'+aligns[['GEOHASH','TEXT']].to_csv(index=False,header=False,mode=str))
		with open(csvfilename,'a') as f:
			f.write('\n'+totalneighbors[['NEIGHBORS','TEXT']].to_csv(index=False,header=False,mode=str)+'\n'+aligns[['GEOHASH','TEXT']].to_csv(index=False,header=False,mode=str))	


	if multiprocessing == False:
		# making lines mask for no reason
		linemask1,linemask2 = make_line_mask(data)

		# make line index metadata
		metadata = make_meta_lines(8,9,len(data),extrema)
		df = pd.DataFrame(['stringdf',json.dumps(linemask2),json.dumps(metadata)],index=['ultindex','areamask','metadata'])
		
		# writing output to h5 file
		if not h5filename == False:
			with pd.HDFStore(h5filename) as out:
				out['combined'] = df
				out['alignmentdf'] = data


		print('Made output h5 file containing datastructures:')
		print('\t- alignmentdf (type: pd.DataFrame)')
		print('\t- areamask (type: dict)')
		print('\t- ultindex (type: dict)')
		print('\t- metadata (type: dict)')


# for a given set of coordinates 
# iterates through each set of points in a line
# and windows through grabbing midpoints on the way
def expand_out(coords,hashsize,name,maxdistance):	
	# setting initial distance
	dist = 0.

	# lines
	lats = []
	longs = []
	dists = []

	count = 0
	for long,lat in coords:
		if count == 0:
			count = 1
		else:
			# getting distance about two points
			dist = dist + distance([oldlong,oldlat],[long,lat])
			
			# getting the number points to iterate through
			number = (int(dist / hashsize) *2) + 2
			if count == 1:
				count = 2
			
			ptlats = np.linspace(oldlat,lat,number)
			ptlongs = np.linspace(oldlong,long,number)
			distances = np.linspace(old_dist,dist,number)
			
			# appending all the outerlist additions
			lats.append(ptlats)
			longs.append(ptlongs)
			dists.append(distances)
		oldlong,oldlat,old_dist = long,lat,dist
	
	lats,longs,dists = np.concatenate(tuple(lats)),np.concatenate(tuple(longs)),np.concatenate(tuple(dists))
			
	my_func = np.vectorize(geohash.encode,excluded=['precision'])
	ghashs = my_func(latitude=lats,longitude=longs,precision=9)


	# getting unique indicies
	indexes = np.unique(ghashs, return_index=True)[1]
	ghashs = np.column_stack((lats,longs,dists,ghashs))
	ghashs = [ghashs[index] for index in sorted(indexes)]
	
	return [[i[-1],'%s,%s,%s,%s' % (i[-1],name,i[-2],maxdistance)] for i in ghashs]

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

def make_line_indexdf(index):
	data = pd.DataFrame(index['ultindex'].items(),columns=['GEOHASH','AREA'])
	data[['OLDGEOHASH','LINEID','DISTANCE','MAXDISTANCE']] = data['AREA'].str.split(',',expand=True)
	return data

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
'''
import mapkit as mk
import pandas as pd
import time
from ult.polygon_dict import get_filetype
counter = 0
for i in get_filetype('csv','csv')[1:]:
	filename = i
	counter += 1
	print counter
	#data = mk.get_database('routes')
	try:
		data = pd.read_csv(i)
	except:
		data = []
	hdfilename = 'indexs/' + i[4:-3] + 'h5'
 	newlist = []
	if not len(data) == 0:
		for i in data['osmid']:
			if '[' == str(i)[0] and ']' == str(i)[-1]:
				i = sum([int(i) for i in str.split(i[1:-1],',')])
			newlist.append(i)

		data['gid'] = newlist
		data['LINEID'] = newlist
		newlist = []
		for i in data['coords'].values:
			newlist.append(get_max_distance(get_cords_json(i)))
		data['maxdistance'] = newlist

		data.to_csv('new/' + str.split(str(filename),'/')[1],index=False)
		s = time.time()
		print hdfilename
		make_line_index(data,str(hdfilename),processes=8,appendsize=250)
		print(time.time() - s)


mk.cln()
mk.make_map(alignmentdf,'')
mk.a()
'''