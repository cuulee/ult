'''
Purpose: This module is to be used for geospatial aggregations on vector data

Module: aggregations.py 

Created by: Bennett Murphy
'''
import itertools
import pandas as pd
import numpy as np
import geohash
import math

#takes a list and turns it into a datafrae
def list2df(df):
    df = pd.DataFrame(df[1:], columns=df[0])
    return df

#takes a dataframe and turns it into a list
def df2list(df):
    df = [df.columns.values.tolist()]+df.values.tolist()
    return df

# getting lat and long for each point
def getlatlong(header,row):
	count=0
	oldrow=row
	for row in header:
		if 'lat' in str(row).lower():
			latpos = count
		elif 'long' in str(row).lower():
			longpos = count
		count+=1

	lat = float(oldrow[latpos])
	long = float(oldrow[longpos])

	return [long,lat]



#function that writes csv file to memory
def writecsv(data, location):
    import csv
    with open(location, 'wb') as f:
        a = csv.writer(f, delimiter=',')
        for row in data:
                if row >= 0:
                        a.writerow(row)
                else:
                        a.writerows(row)
    print 'Wrote csv file to location: %s' % location

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
		for row in geometry:
			row=str.split(row,' ')
			distance=float(row[-1])
			lat=float(row[1])
			long=float(row[0])
			try: 
				hash = geohash.encode(float(lat), float(long), precision)
				newrow=newvalues+[lat,long,distance,hash]
				newtable.append(newrow)
			except Exception:
				pass

	except Exception:
		newtable=[['GEOHASH'],['']]

	# taking table from list to dataframe
	newtable=list2df(newtable)

	return np.unique(newtable['GEOHASH']).tolist()

# returning dictionary with a unique identifier and geohashed squares occuring on line vector data
def make_line_dict(table,precision,position):
	# where table is dataframe table
	# where precision is the precision of the geohash
	# where position is the unique identifier dictioniary integer positon in each row 
	# return dict entry for each line segment {identifier:[geohash list]}
	data=table

	if isinstance(data,pd.DataFrame):
		data=df2list(data)
	header=data[0]

	count=0
	for row in data[1:]:
		temp=extend_geohashed_table(header,row,precision)
		uniques=np.unique(temp['GEOHASH']).tolist()
		if count==0:
			count=1
			uniquedict={row[position]:uniques}
		else:
			uniquedict[row[position]]=uniques
	return uniquedict

# returning DataFrame with ever geohashed square and every routeid with each one
def make_line_frame(table,precision,position,**kwargs):
	# where table is dataframe table
	# where precision is the precision of the geohash
	# where position is the unique identifier dictioniary integer positon in each row 
	# return dict entry for each line segment {identifier:[geohash list]}
	# csv is a bool that returns a csv file or tries to read a csv file if one is available if not it will create and write one
	csv = False
	data=table
	if kwargs is not None:
		for key,value in kwargs.iteritems():
			if key == 'csv':
				if value == True:
					csv = True

	# doing dataframe logic
	if isinstance(data,pd.DataFrame):
		data=df2list(data)
	header=data[0]
	columnheader = header[position]
	newtable = [['GEOHASH',columnheader]]

	count=0
	count2=0
	total=0

	if csv == True:
		try: 
			newtable = pd.read_csv('line_frame'+str(precision)+'.csv')
			return newtable
		except Exception:
			print 'No line frame csv file found, creating line frame.'
	for row in data[1:]:
		count2+=1
		uniques = extend_geohashed_table(header,row,precision)
		temptable = ['GEOHASH']+uniques
		temptable = pd.DataFrame(temptable[1:], columns=[temptable[0]])
		temptable[columnheader] = row[position]
		temptable = df2list(temptable)
		newtable += temptable[1:]
		if count2 == 1000:
			total+=count2
			count2=0
			print '[%s/%s]' % (total,len(data))

	if csv == True:
		writecsv(newtable,'line_frame'+str(precision)+'.csv')

	return list2df(newtable)

# generator for a line dictionary 
def gen_linedict_keys(linedict):
	for row in linedict.keys():
		yield row

# from a line dictionary like the one created above returns the first instance of a geohash being found within 
# any of the dictionary entries
def get_uniqueid_linedict(linedict,geohash):
	nextkey=gen_linedict_keys(linedict)
	found=False
	while found==False:
		try: 
			key=next(nextkey)
			linedictlist=linedict[key]
			for row in linedictlist:
				if row==geohash:
					found=True
					uniqueid=key
					return uniqueid
		except Exception:
			return ''

# concatenates two like dataframes
def concatenate_tables(table1,table2):
	header1 = table1.columns.tolist()
	header2 = table2.columns.tolist()
	frames = [table1,table2]

	if header1 == header2:
		data = pd.concat(frames)
		return data


# bind a geohashed table of occurances to vector data by unique column input
def bind_geohashed_data_dict(uniqueid,linedict,geohashed_table,vector_database):
	data = vector_database
	# iterating through each traffic fatility and getting route
	total = [[uniqueid,'COUNT']]
	uniques=[]
	for row in df2list(geohashed_table)[1:]:
		hash = row[-1]
		unique = get_uniqueid_linedict(linedict,hash)
		total.append([unique,1])
		uniques.append(unique)

	total = list2df(total)

	uniques = np.unique(uniques).tolist()
	# taking away null values returned 
	if uniques[0] == '':
		uniques = uniques[1:]

	total = total.groupby([uniqueid],sort=True).sum()
	total = total.reset_index()
	total = df2list(total)

	count=0
	for row in total[1:]:
		if count == 0:
			totaldict = {row[0]:row[1]}
			count=1
		else: 
			totaldict[row[0]] = row[1]

	# taking total and creating a new dataframe from uniqueids present
	total = total[1:]
	dataheader = data.columns.tolist()
	data['BOOL'] = data[uniqueid].isin(uniques)
	data = data[data.BOOL == True]
	data = data[dataheader]
	data = df2list(data)

	# setting up header
	newdata=[data[0] + ['COUNT']]

	# getting uniqueid positon number
	count=0
	for row in data[0]:
		if row == uniqueid:
			position=count
		count+=1

	# iterating through data
	for row in data[1:]:
		print row
		key = row[position]
		value = totaldict[key]
		newrow = row + [value]
		newdata.append(newrow)

	# taking back to df
	newdata = list2df(newdata)

	return newdata

# given a unique column id (column header), a lineframe, and geohashed_data, and a vector database
# returns a dataframe of aggregated linesegments by count of occurence from geohashed data
def bind_geohashed_data_frame(uniqueid,lineframe,geohashed_data,vector_database):
	data = vector_database

	# getting unique geohashs
	uniques = np.unique(geohashed_data['GEOHASH']).tolist()

	# creating a dataframe with only applicable geohashs to get uniqueids found
	newdata = querry_multiple(lineframe,'GEOHASH',uniques)

	# grouping by routeid and then creating a dictionary 
	newdata['COUNT'] = 1 
	groupeddata = newdata[[uniqueid,'COUNT']]
	groupeddata = groupeddata.groupby([uniqueid],sort=True).sum()
	groupeddata = groupeddata.reset_index()
	uniqueids = np.unique(groupeddata[uniqueid]).tolist()
	groupeddata = df2list(groupeddata)
	

	# creating a dictionary of grouped data to use the keys to create
	count=0
	for row in groupeddata[1:]:
		if count == 0:
			totaldict = {row[0]:row[1]}
			count=1
		else: 
			totaldict[row[0]] = row[1]

	# creating dataframe of vector data using only routeids found
	specificdata = querry_multiple(data,uniqueid,uniqueids)

	# getting header and adding "COUNT" value
	header = specificdata.columns.tolist() + ['COUNT']

	# setting up newtable from header
	newtable = [header]

	# getting uniqueid positon number
	data = df2list(data)
	count=0
	for row in data[0]:
		if row == uniqueid:
			position=count
		count+=1

	# iterating through specific data and adding the appropriate count to each column 
	for row in df2list(specificdata)[1:]:
		key = row[position]
		value = totaldict[key]
		newrow = row + [value]
		newtable.append(newrow)

	# taking back to df
	newtable = list2df(newtable)

	return newtable

# given two points gets linear line between the two 
# returns slope and y intercept
def get_linear_line_vars(oldpoint,point):
	try: 
		slope = (point[1] - oldpoint[1]) / (point[0] - oldpoint[0])
	except ZeroDivisionError:
		slope = 0
	b = point[1] - slope*point[0]
	return [slope,b]


# this function takes a dataframe table a column header, and a list objects and sets returns only rows
# containing one of the values in the list in the column header given
def querry_multiple(table,headercolumn,list):
	data=table
	dataheader = data.columns.tolist()
	data['BOOL'] = data[headercolumn].isin(list)
	data = data[data.BOOL == True]
	data = data[dataheader]
	data.columns = dataheader
	return data

# from the output of get_linear_line_vars() and the current point returns perpendicular line vars
def get_perpendicular_line(point,vars):
	slope = (vars[0]**-1)*-1
	b = point[1] - point[0]*slope
	return [slope,b]

# gets a polygon from a line string 
def get_polygon_fromline(data):
	# taking data frame to list 
	if isinstance(data,pd.DataFrame):
		data = df2list(data)

	newlist = [['LONG', 'LAT']]
	newlist2 = [['LONG', 'LAT']]
	window = 2.0110324228e-04

	for row in data[1:]:
		point = getlatlong(data[0],row)
		point1 = [point[0]+window,point[1] + window]
		point2 = [point[0]-window,point[1] - window]		
		newlist.append(point1)
		newlist2.append(point2)

	newlist2 = newlist2[1:]
	for row in reversed(newlist2):
		newlist.append(row)
	newlist.append(newlist[1])
	return newlist




# extends a new table down from a post gis row and header
def extend_geohashed_table2(header,extendrow,precision,**kwargs):
	count=0
	newheader=[]
	newvalues=[]
	return_dataframe = False
	for key,value in kwargs.iteritems():
		if key == 'return_dataframe':
			if value == True:
				return_dataframe = True


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
	geometry=extendrow[geometrypos]
	geometry=str.split(geometry,'(')
	geometry=geometry[-1]
	geometry=str.split(geometry,')')
	geometry=geometry[:-2][0]
	geometry=str.split(geometry,',')

	count = 0

	points = [['LONG','LAT']]
	# setting up new table that will be returned as a dataframe
	newtable=[newheader]
	for row in geometry:
		row=str.split(row,' ')
		distance=float(row[-1])
		lat=float(row[1])
		long=float(row[0])
		point = [long,lat]
		points.append(point)
	points = get_polygon_fromline(points)
	for row in points[1:]:
		lat = row[1]
		long = row[0]

		if count == 0: 
			count = 1
			hash = geohash.encode(float(lat), float(long), precision)
			newrow=newvalues+[lat,long,distance,hash]
			newtable.append(newrow)	
		else:
			hash = geohash.encode(float(lat), float(long), precision)
			# logic for checking distance between last point 
			# then adding points that make up the points in between the two values
			newrow=newvalues+[lat,long,distance,hash]

			oldpoint = getlatlong(newheader,oldrow)
			newpoint = getlatlong(newheader,newrow)
			distance = ((newpoint[0] - oldpoint[0])**2 + (newpoint[1] - oldpoint[1])**2)**.5
			distance = abs(distance)
			linevars = get_linear_line_vars(oldpoint,newpoint)

			# checking line vars
			if linevars[0] == 0:
				distance = 0
			

			if distance >= .0001:
				# getting the linevars (slope and b) and then getting the degrees
				
				deginrad = math.atan(linevars[0])
				pointstoadd = []

				# the step size of each iteration for x and y 
				xstep =  .0001 * math.cos(deginrad)
				ystep =  .0001 * math.sin(deginrad)
				# getting the current vlaues of x and y 
				currentx = oldpoint[0]
				currenty = oldpoint[1]


				# checking to see whether or not xstep and y step needs to be inverted to 
				# to reach desired point
				if oldpoint[1] < newpoint[1] and ystep < 0:
					xstep = xstep*-1
					ystep = ystep*-1

				# iterating through the distance and adding points at the needed precision
				currentdistance = 0
				while currentdistance < distance:
					currentdistance += .0001
					currentx += xstep
					currenty += ystep
					hash = geohash.encode(float(currenty), float(currentx), precision)
					pointstoadd.append(newvalues+[currenty,currentx,0,hash])
				
				if len(pointstoadd)==2:
					if distance > .0002:
						newtable += pointstoadd	
				else: 
					newtable += pointstoadd

					
				newtable.append(newrow)


		oldrow = newrow



	# taking table from list to dataframe
	newtable=list2df(newtable)

	if return_dataframe == True:
		return newtable
	return np.unique(newtable['GEOHASH']).tolist()

# returning DataFrame with ever geohashed square and every routeid with each one
def make_line_frame2(table,precision,position,**kwargs):
	# where table is dataframe table
	# where precision is the precision of the geohash
	# where position is the unique identifier dictioniary integer positon in each row 
	# return flat dataframe with routeid (unique id.)/geohash values for each list in a database
	# csv is a bool that returns a csv file or tries to read a csv file if one is available if not it will create and write one
	csv = False
	data=table
	if kwargs is not None:
		for key,value in kwargs.iteritems():
			if key == 'csv':
				if value == True:
					csv = True

	# doing dataframe logic
	if isinstance(data,pd.DataFrame):
		data=df2list(data)
	header=data[0]
	columnheader = header[position]
	newtable = [['GEOHASH',columnheader]]

	count=0
	count2=0
	total=0

	if csv == True:
		try: 
			newtable = pd.read_csv('line_frame'+str(precision)+'.csv')
			return newtable
		except Exception:
			print 'No line frame csv file found, creating line frame.'
	for row in data[1:]:
		count2+=1
		uniques = extend_geohashed_table2(header,row,precision)
		temptable = ['GEOHASH']+uniques
		temptable = pd.DataFrame(temptable[1:], columns=[temptable[0]])
		temptable[columnheader] = row[position]
		temptable = df2list(temptable)
		newtable += temptable[1:]
		if count2 == 1000:
			total+=count2
			count2=0
			print '[%s/%s]' % (total,len(data))

	if csv == True:
		writecsv(newtable,'line_frame'+str(precision)+'.csv')

	return list2df(newtable)



