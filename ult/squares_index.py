import pandas as pd
import numpy as np
import berrl as bl
import itertools

#takes a list and turns it into a datafrae
def list2df(df):
    df = pd.DataFrame(df[1:], columns=df[0])
    return df

#takes a dataframe and turns it into a list
def df2list(df):
    df = [df.columns.values.tolist()]+df.values.tolist()
    return df

# getting lat and long for each point
def getlatlongheader(header):
	count=0
	for row in header:
		if 'lat' in str(row).lower():
			lat = row
		elif 'long' in str(row).lower():
			long = row
		count+=1

	return [lat,long]

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




# getting extream of all point cloud data
def get_extrema(data):
	# getting the header
	header = df2list(data)[0]

	# getting the lat and long header respectively
	latheader, longheader = getlatlongheader(header)

	# getting the extrema with df ops
	xmin = data[longheader].min()
	xmax = data[longheader].max()
	ymin = data[latheader].min()
	ymax = data[latheader].max()

	# putting it in a dict
	extremadict = {'w':xmin,'e':xmax,'s':ymin,'n':ymax}

	return extremadict

# given a dimmension min and max and round precision return a round up
# and returns the delta points between those 
def get_rounds(min,max,roundprecision):
	# checking to see if min is really less than max if not switches the too
	if min > max:
		oldmin = min 
		min = max
		max = oldmin

	# getting the round up values for the end points
	minround = round(min,roundprecision)
	maxround = round(max,roundprecision)

	# getting the delta to be traversed
	delta =  float(10 ** -roundprecision)
	rounds = [minround]

	# traversing through the range
	current = minround
	while current < maxround:
		current += delta
		addcurrent = round(current,roundprecision+1)
		rounds.append(addcurrent)

	# adding last point 
	rounds.append(maxround)

	return rounds



# for a given dataframe alignment and point
# returns a list of 3 points being:
#	* the point that occured before the closest 
#	* the closest point 
# 	* the point after the closest point
def get_closet_neighbors(point,data):
	# getting the header
	header = df2list(data)[0]

	# getting the lat and long header respectively
	latheader, longheader = getlatlongheader(header)

	# creating a  distance column
	data['DIST'] = ((point[0]-data[longheader])**2+(point[1]-data[latheader])**2)**.5

	# minimum value
	minimum = data['DIST'].min()

	# getting the closest point row
	minimumrow = data[data.DIST == minimum]

	# minimum row removing dataframe and header
	minimumrow = df2list(minimumrow)[1]

	# getting the adjacent points in the alignment list
	data = df2list(data)
	header = data[0]
	oldrow = []
	oldoldrow = []
	count = 0
	for row in data[1:]:
		if count==0:
			count=1
		elif count==1:
			count=2
			if oldrow == minimumrow:
				neighbors = [oldrow,row]
		else:
			if oldrow == minimumrow:
				neighbors = [oldoldrow,oldrow,row]
		
		oldoldrow = oldrow
		oldrow = row

	neighbors = [header] + neighbors

	return list2df(neighbors)


# given a float returns the number of precision 
def get_decimal_precision(floatnumber):
	string = str(floatnumber)
	string = str.split(string,'.')[1]
	precision = len(string)
	return precision 


# given two points gets linear line between the two 
# returns slope and y intercept
def get_linear_line_vars(oldpoint,point):
	slope = (point[1] - oldpoint[1]) / (point[0] - oldpoint[0])
	b = point[1] - slope*point[0]
	return [slope,b]

def solve_table(data,desiredy,extremadict):
	# taking to list
	data = df2list(data)

	# getting header
	header = data[0]

	# getting first point/oldopint window
	oldrow = data[-2]
	oldpoint = getlatlong(header,oldrow)

	# new list for solved points 
	newlist = [['LONG','LAT']]

	# iterating through each point in the alignment
	for row in data[1:]:

		point = getlatlong(header,row)
		linevars = get_linear_line_vars(oldpoint,point)

		# logic for the to get the highest and lowest y values of the two points
		if oldpoint[1] > point[1]:
			ymax = oldpoint[1]
			ymin = point[1]
		elif oldpoint[1] <= point[1]:
			ymax = point[1]
			ymin = oldpoint[1]

		if data[-1] == row:
			desiredprecision = get_decimal_precision(desiredy)
			ymax = round(extremadict['n'],desiredprecision+2)
			

		solvedpoint = solvey(linevars,desiredy)


		if not solvedpoint == None:
			if solvedpoint[1] < ymax and solvedpoint[1] >= ymin:
				newlist.append(solvedpoint)
		if row == data[-1]:
			# for the last value in a polygon checks values to see if two values exist
			# if two solved values exists uses those
			if len(newlist)>=3:
				uniques = []
				solved = newlist

				# iterating through each solved solution
				for row in solved:
					ind = 0
					oldrow =row
					for row in uniques:
						if oldrow == row:
							ind=1
					if not ind==1:
						uniques.append(oldrow)
				return list2df(uniques)
		oldpoint = point 
		oldrow = row

	# getting xmin and xmax from extrema dictionary
	xmin = extremadict['w']
	xmax = extremadict['e']


	# taking new list to dataframe
	newlist = list2df(newlist)

	# getting lat and long header
	latheader,longheader = getlatlongheader(header)

	# querrying out point that dont fit in xmin or xmax
	newlist = newlist[(newlist[longheader] >= xmin)&(newlist[longheader] <= xmax)]

	return newlist

# solving for y given line a list of line variables slope and b
def solvey(linevars,y):
	# getting slope and b
	slope = linevars[0]
	b = linevars[1]

	x = (y - b)/ slope

	return [x,y]

# essentially zips the xrounds and yrounds into a list 
def construct_xy(xrounds,yrounds):
	points = [['LONG','LAT']]
	# iterating trhough each x round and y round and append the point
	for x,y in itertools.izip(xrounds,yrounds):
		points.append([x,y])
	return points

# given a two lists iterates through each and returns 
def constructx_fromlines(x1,x2):
	likelist = []
	if len(x1) > len(x2):
		for row in x1:
			oldrow = row
			for row in x2:
				if oldrow == row:
					likelist.append(row)
	elif len(x1) <= len(x2):
		for row in x2:
			oldrow = row
			for row in x1:
				if oldrow == row:
					likelist.append(row)

	return likelist

def constructy_fromsize(yvalue,sizeoflist):
	yrounds = []

	while not sizeoflist == len(yrounds):
		yrounds.append(yvalue)
	return yrounds

# creates a geospatial index 
def make_geospatial_index(data,roundprecision,**kwargs):
	csv = False
	for key,value in kwargs.iteritems():
		if key == 'csv':
			if value == True:
				csv = True

	if csv == True:
		try:
			squares = pd.read_csv('square_index'+str(roundprecision)+'.csv')
			return squares
		except Exception:
			pass


	# getting dictionnary extrema
	extremadict = get_extrema(data)


	ymin = extremadict['s']
	ymax = extremadict['n']

	xmin = extremadict['w']
	xmax = extremadict['e']

	yrounds = get_rounds(ymin,ymax,roundprecision)

	totalpoints=[['LONG','LAT']]
	count = 0
	count3 = 0
	squares = [['GEOHASH','LAT1','LONG1','LAT2','LONG2','LAT3','LONG3','LAT4','LONG4']]
	for row in yrounds[1:]:
		y = row

		# getting solved points
		points = solve_table(data,y,extremadict)

		# making sure a points value was returned
		if len(points) == 2:
			# taking point to list
			points = df2list(points)


			# getting x min and x maximimum
			xmin = points[1]
			xmin = xmin[0]
			xmax = points[2]
			xmax = xmax[0]

			xrounds = get_rounds(xmin,xmax,roundprecision)
			yrounds = constructy_fromsize(y,len(xrounds))

			if count ==0:
				count=1
			else:
				if len(oldx) == len(xrounds) and oldx == xrounds:
					for row in oldx:
						oldpoints = construct_xy(oldx,oldy)
						points = construct_xy(oldx,yrounds)
				else:
					# getting x values that will be iterated through two construct squares
					xroundsused = constructx_fromlines(oldx,xrounds)

					# getting new ys from because the size of the line has changed
					oldy = constructy_fromsize(oldy[0],len(xroundsused))
					yrounds = constructy_fromsize(yrounds[0],len(xroundsused))

					# getting the actual points associated with the square for each vertical line
					oldpoints = construct_xy(xroundsused,oldy)
					points = construct_xy(xroundsused,yrounds)

				count2 = 0 
				bottompoints = oldpoints
				toppoints = points
				# iterating through each set of values 
				for top,bottom in itertools.izip(bottompoints[1:],toppoints[1:]):
					if count2 == 0:
						count2 =1
					else:
						count3 +=1
						bottomright = bottom
						bottomleft = oldbottom
						topright = top
						topleft = oldtop
						newrow = [count3,bottomright[1],bottomright[0],bottomleft[1],bottomleft[0],topright[1],topright[0],topleft[1],topleft[0]]
						squares.append(newrow)

					oldbottom = bottom
					oldtop = top


			oldx = xrounds
			oldy = yrounds

	squares = list2df(squares)

	if  csv == True:
		squares.to_csv('square_index'+str(roundprecision)+'.csv',index=False)
		return squares
	else:
		return squares

# this function returns a boolean if the a point 
# is in one of the squares of index table
def ispoint_index(point,data):
	x = point[0]
	y = point[1]
	square = data[(data.LAT1 >= y)&(data.LAT3 <= y)&(data.LONG1 >= x)&(data.LONG2 <= x)]
	if len(square)==0:
		return False
	if not len(square)==0:
		return True

	return square 

# returns a square index from a point and geospatial dataframe given
def return_squareindex(point,data):
	x = point[0]
	y = point[1]
	square = data[(data.LAT1 >= y)&(data.LAT3 <= y)&(data.LONG1 >= x)&(data.LONG2 <= x)]
	if not len(square)==0:
		squareindex = np.unique(square['GEOHASH']).tolist()[0]
		return squareindex
	else:
		return ''



