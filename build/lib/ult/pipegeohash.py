import pandas as pd
import geohash
import time
import itertools
import numpy as np
import random 

'''
This module is meant to be used in conjunction with pipekml however I could see how it could have other uses.

Module: pipegeohash.py

Purpose: A simple tool for geohashing an entire table at once,and allowing you to update your both corresponding tables simultaneously

Functions to be used:
1) map_table(csvfile,presicion)-given a csv file name and a presicion from (1 to 8) return the coresponding geohash
2) df2list(df)-takes a dataframe to a list
3) list2df(list)-takes a list and turn it into DataFrame

Created by: Bennett Murphy
email: murphy214@marshall.edu
'''

#takes a dataframe and turns it into a list
def df2list(df):
    df = [df.columns.values.tolist()]+df.values.tolist()
    return df


#takes a list and turns it into a datafrae
def list2df(df):
    df = pd.DataFrame(df[1:], columns=df[0])
    return df

# encoding the geohash string containing arguments into a function that will be mapped
def my_encode(argstring):
	argstring = str.split(argstring,'_')
	lat,long,precision = float(argstring[0]),float(argstring[1]),int(argstring[2])
	try:
		hash = geohash.encode(lat,long,precision)
	except:
		hash = ''
	return hash

# getting lat and long headers
def get_latlong_headers(headers):
	for row in headers:
		if 'lat' in str(row).lower():
			latheader = row
		elif 'lon' in str(row).lower():
			longheader = row
	return [latheader,longheader]


# given a dataframe and a list of columnsn 
# drops columns from df
def drop_columns(table,columns):
	list = []
	count = 0
	for row in columns:
		columnrow = row
		for row in table.columns.values.tolist():
			if columnrow == row:
				list.append(count)
			count += 1

	table.drop(table.columns[list], axis=1, inplace=True)	
	return table
# function making the geohash string and returning the table with an 
# appropriate geohash column 
def geohash_table(data,latlongheaders,precision):
	latheader,longheader = latlongheaders
	data['ARGS'] = data[latheader].astype(str) + '_' + data[longheader].astype(str) + '_' + str(precision)
	data['GEOHASH'] = data['ARGS'].map(my_encode)
	data = drop_columns(data,['ARGS'])
	return data

# function making the geohash string and returning the table with an 
# appropriate geohash column 
def geohash_table(data,latlongheaders,precision):
	latheader,longheader = latlongheaders
	data['ARGS'] = data[latheader].astype(str) + '_' + data[longheader].astype(str) + '_' + str(precision)
	data['GEOHASH'] = data['ARGS'].map(my_encode)
	data = drop_columns(data,['ARGS'])
	return data

# function to perform outer (column) operations on fields and occasonially a function map (maybe)
def perform_outer_operation(data,field,operation):
	derivfield = str.split(field,'_')[0]
	print derivfield,operation
	# right now ill do the 3 stat operations taht seem relevant
	if operation.lower() == 'mean':
		data[field] = data[derivfield].mean()
	elif operation.lower() == 'sum':
		data[field] = data[derivfield].sum()
	elif operation.lower() == 'std':
		data[field] = data[derivfield].std()
	elif operation.lower() == 'max':
		data[field] = data[derivfield].max()
	return data


# makes and returning the squares table.
def make_squares(data,precision,columns):
	# doing the grop by and sorting by the highest count value
	data['COUNT'] = 1
	squares = data[['GEOHASH','COUNT']]
	squares = squares.groupby('GEOHASH').sum()
	squares = squares.sort(['COUNT'],ascending=[0])
	squares = squares.reset_index()
	squares['GEOHASH'] = squares['GEOHASH'].astype(str)
	squares = squares[squares.GEOHASH.str.len() > 0]
	squares = squares.groupby('GEOHASH').first()
	squares = squares.reset_index()

	# making header
	header =  ['GEOHASH','LAT1', 'LONG1', 'LAT2', 'LONG2', 'LAT3', 'LONG3', 'LAT4', 'LONG4','COUNT'] 
	newsquares = [header]
	# iterating through each square here 
	for row in df2list(squares)[1:]:
		# getting points
		points = get_points_geohash(row[0])
		
		# making new row
		newrow = [row[0]] + points + row[1:]
 		
 		# appending to newsquares 
 		newsquares.append(newrow)

 	# taking newsquares to dataframe
 	squares = list2df(newsquares)

	return squares

# given a geohash returns the 4 points that will make up the squares table
def get_points_geohash(hash):
    #processing out the 4 points
    hashreturn = geohash.decode_exactly(hash)

    #getting lat and long datu
    latdatum = hashreturn[0]
    longdatum = hashreturn[1]

    #getting delta
    latdelta = hashreturn[2]
    longdelta = hashreturn[3]

    point1 = [latdatum-latdelta, longdatum+longdelta]
    point2 = [latdatum-latdelta, longdatum-longdelta]
    point3 = [latdatum+latdelta, longdatum+longdelta]
    point4 = [latdatum+latdelta, longdatum-longdelta]

    return point1 + point2 + point3 + point4


# given a geohash returns the 4 points that will make up the squares table
def get_alignment_geohash(hash):
    #processing out the 4 points
    hashreturn = geohash.decode_exactly(hash)

    #getting lat and long datu
    latdatum = hashreturn[0]
    longdatum = hashreturn[1]

    #getting delta
    latdelta = hashreturn[2]
    longdelta = hashreturn[3]

    point1 = [latdatum-latdelta, longdatum+longdelta]
    point2 = [latdatum-latdelta, longdatum-longdelta]
    point3 = [latdatum+latdelta, longdatum-longdelta]
    point4 = [latdatum+latdelta, longdatum+longdelta]

    return [point1,point2,point3,point4,point1]


# gets all relevant headers for each value in columns
def get_column_headers(columnsandcount,headers):
	columnheaders = []
	for row in columnsandcount:
		oldrow = str(row)
		for row in headers:
			if oldrow in str(row):
				columnheaders.append(row)
	return columnheaders

# given columnsheaders from output above
# checks if the output of str split is above 2 
# if it is another operation is supposed to be performed
# in a way an api for other non sum values I guess that 
# extra field will be used to determine whether a operation is outer or inner
# group by if a field is adde, thinking aout loud
def get_nonsum_headers(columnheaders):
	nonsumheaders = []
	for row in columnheaders:
		if '_' in str(row):
 			nonsumheaders.append(row)
 	return nonsumheaders


# does the inverse of operaton above
def get_sum_headers(columnheaders):
	sumheaders = []
	for row in columnheaders:
		if not '_' in str(row):
 			sumheaders.append(row)
 	return sumheaders

# creates geohash and squares table
def map_table(data,precision,**kwargs):
	columns = []
	filename = False
	return_squares = False
	map_only = False
	geohash_field = False
	latlongheaders = False 

	for key,value in kwargs.iteritems():
		if key == 'columns':
			columns = value
		if key == 'filename':
			filename = value
		if key == 'return_squares':
			return_squares = value
		if key == 'map_only':
			map_only = value
		if key == 'geohash_field':
			geohash_field = value
		if key == 'latlongheaders':
			latlongheaders = value


	if geohash_field  == False:
		# sending into new geohashing function
		data = geohash_points(data,precision,latlongheaders)
	else:
		data['GEOHASH'] = data['GEOHASH'].str[:precision]


	# returning data if only the mapped table should be returned 
	if map_only == True:
		return data

	# getting column headers
	columnheaders = data.columns.values.tolist()
	
	# making squares table
	squares = make_squares(data,8,columnheaders)

	if not filename == False:
		squares.to_csv(filename,index=False)
	else:
		squares.to_csv('squares' +str(precision) + '.csv',index=False)

	if return_squares == True:
		return squares
	else:
		return data


# given a table with a high geohash a list of precisions creates consitutent
# tables and writes out to csv files accordingly
# input the columns field for other values to be summed or grouped by 
def make_geohash_tables(table,listofprecisions,**kwargs):
	'''
	sort_by - field to sort by for each group
	return_squares - boolean arg if true returns a list of squares instead of writing out to table
	'''
	return_squares = False
	sort_by = 'COUNT'
	# logic for accepting kwarg inputs
	for key,value in kwargs.iteritems():
		if key == 'sort_by':
			sort_by = value
		if key == 'return_squares':
			return_squares = value

	# getting header
	header = df2list(table)[0]

	# getting columns
	columns = header[10:]

	# getting original table
	originaltable = table
	if not sort_by == 'COUNT':
		originaltable = originaltable.sort([sort_by],ascending=[0])




	listofprecisions = sorted(listofprecisions,reverse=True)
	# making total table to hold a list of dfs
	if return_squares == True and listofprecisions[-1] == 8:
		total_list = [table]
	elif return_squares == True:
		total_list = []

	for row in listofprecisions:
		precision = int(row)
		table = originaltable
		table['GEOHASH'] = table.GEOHASH.str[:precision]
		table = table[['GEOHASH','COUNT']+columns].groupby(['GEOHASH'],sort=True).sum()
		table = table.sort([sort_by],ascending=[0])
		table = table.reset_index()

		newsquares = [header]
		# iterating through each square here 
		for row in df2list(table)[1:]:
			# getting points
			points = get_points_geohash(row[0])
			
			# making new row
			newrow = [row[0]] + points + row[1:]
			
			# appending to newsquares 
			newsquares.append(newrow)

		# taking newsquares to dataframe
		table = list2df(newsquares)

		if return_squares == True:
			total_list.append(table)
		else:
			table.to_csv('squares' + str(precision) + '.csv',index=False)

	if return_squares == True:
		return total_list
	else:
		print 'Wrote output squares tables to csv files.'


# given a list of geohashs returns a dataframe that can be 
# sent into make blocks 
def make_geohash_blocks(geohashs,**kwargs):
	df = False
	for key,value in kwargs.iteritems():
		if key == 'df':
			df = value
	if df == True:
		geohashs = geohashs.unstack(level=0).reset_index()[0].values.tolist()
	header = ['GEOHASH','LAT1', 'LONG1', 'LAT2', 'LONG2', 'LAT3', 'LONG3', 'LAT4', 'LONG4','COUNT']
	newlist = [header]

	for row in geohashs:
		if not row == '':
			points = get_points_geohash(row)
			newrow = [row] + points + [1]
			newlist.append(newrow)

	return list2df(newlist)

# given a table of points and geohashs returns the same table with indicies positon in each geohash
# from indicies get decimal points
def ind_dec_points(alignmentdf):
	# getting alignment df
	header = alignmentdf.columns.values.tolist()

	count =0
	for row in header:
		if 'lat' in row.lower():
			latpos = count
		elif 'long' in row.lower():
			longpos = count
		elif 'geohash' in row.lower():
			hashpos = count
		count += 1
	xs = []
	ys = []
	for row in alignmentdf.values.tolist():
		lat = row[latpos]
		long = row[longpos]
		ghash = row[hashpos]
		
		midlat,midlong,latdelta,longdelta = geohash.decode_exactly(ghash)
		ulcornerpoint = [midlat + latdelta,midlong - longdelta]

		latsize = latdelta * 2
		longsize = longdelta * 2

		x = abs(ulcornerpoint[1] - long) / longsize
		y = abs(ulcornerpoint[0] - lat) / latsize

		xs.append(x)
		ys.append(y)

	alignmentdf['x'] = xs
	alignmentdf['y'] = ys

	return alignmentdf
# mapped function for creating geohahs center points
def make_geohash_point(ghash):
	lat,long = geohash.decode(ghash)
	return [long,lat]

# creates center point df from a unique geohash list
def points_from_geohash(geohashlist):
	data = pd.DataFrame(geohashlist,columns=['GEOHASH'])
	holder = data['GEOHASH'].apply(make_geohash_point)
	data[['LONG','LAT']] = pd.DataFrame(holder.values.tolist(),columns=['LONG','LAT'])
	return data

# creates center point df from a unique geohash list
def points_from_geohash4(geohashlist):
	total = [['GEOHASH','LONG','LAT']]
	for row in geohashlist:
		y,x,yd,xd = geohash.decode_exactly(row)
		pt1 = [row,x+xd,y+yd] # ne
		pt2 = [row,x-xd,y-yd] # sw
		pt3 = [row,x+xd,y-yd] # se
		pt4 = [row,x-xd,y+yd] # nw
		total += [pt1,pt2,pt3,pt4]
	total = pd.DataFrame(total[1:],columns=total[0])
	return total

# the second part of the actual geohashing process
# where the actual geohashing occurs
def geohash_linted(lats,lngs,precision):
	newlist = []
	ds = []
	for i in range(0,len(lats)):
		oi = (lats[i],lngs[i],precision)
 		#newlist.append(oi)
 		ds.append(geohash.encode(*oi))
 		#for i in range(0,len(pts)):
		#ds.append(geohash.encode(*newlist[i]))
	return ds


# lints points for non hashable data types
def lint_values(data,latlongheaders):
	if not latlongheaders == False:
		lathead,longhead = latlongheaders
	else:	
		for row in data.columns.values.tolist():
			if 'lat' in str(row).lower():
				lathead = row
			elif 'long' in str(row).lower():
				longhead = row
	
	data = data[(data[lathead] < 90.0) & (data[lathead] > -90.0)]
	data = data.fillna(value=0)

	return data[lathead].astype(float).values.tolist(),data[longhead].values.tolist(),data


# performs both operations above
# may accept a kwarg to throw the output geohash into an area function letter
def geohash_points(data,precision,latlongheaders):
	# selecting the point values that can be geohashed 
	#meaning anything under or above 90 to - 90
	lats,longs,data = lint_values(data,latlongheaders)
	data['GEOHASH'] = geohash_linted(lats,longs,precision)
	return data


# given a number of points in which to generate 
# returns a random number of lat and longs for testing etc.
# this function is encapsulated so that its not easier to just geohash
# returns df with fields lat,long
def random_points(number):
	os = []
	for i in range(number):
		o = ((random.random()*2 - 1.0)*90.0, (random.random()*2 - 1.0)*180.0 )
		os.append(o)
	
	return pd.DataFrame(os,columns = ['LAT','LONG'])

def latval(latitude):
	if latitude > 0:
		return (latitude / 90.0) / 2.0 + .5
	else:
		return .5 - abs((latitude / 90.0) / 2.0 )

def longval(longitude):
	if longitude > 0:
		return (longitude / 180.0) / 2.0 + .5
	else:
		return .5 - abs((longitude / 180.0) / 2.0 )


# given an extrema retreives rangdom points within extrema to be generated
def random_points_extrema(number,extrema):
	os = []
	latmax = extrema['n']
	latmin = extrema['s']
	longmin = extrema['w']
	longmax = extrema['e']

	decx1 = latval(latmin)
	decx2 = latval(latmax)

	decy1 = longval(longmin)
	decy2 = longval(longmax)


	minlat = 30.0
	for i in range(number):
		o = ((random.uniform(decx1,decx2)*2 - 1)*90, (random.uniform(decy1,decy2)*2 - 1.0)*180 )
		os.append(o)
	
	return pd.DataFrame(os,columns = ['LAT','LONG'])

