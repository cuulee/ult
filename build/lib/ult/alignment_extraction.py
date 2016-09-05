'''
Purpose: This module is for extracting polygon alignments from point clouds in a reliable manner.

Module: aggregations.py 

Created by: Bennett Murphy
'''
import pandas as pd
import numpy as np
import berrl as bl

#takes a list and turns it into a datafrae
def list2df(df):
    df = pd.DataFrame(df[1:], columns=df[0])
    return df

#takes a dataframe and turns it into a list
def df2list(df):
    df = [df.columns.values.tolist()]+df.values.tolist()
    return df

# getting extream of all point cloud data
def get_extrema(dataframe):
	xmin = data['LONG'].min()
	xmax = data['LONG'].max()
	ymin = data['LAT'].min()
	ymax = data['LAT'].max()

	return {'w':xmin,'e':xmax,'s':ymin,'n':ymax}
	
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

# getting all the y values that willl be evaulated
def get_ytraverse(ymin,ymax,delta):
	ytraverse=[]
	current = ymin
	while ymax>current:
		current += delta
		if current > ymax:
			current = ymax
		ytraverse.append(current)
	return ytraverse

# expands out the values in the ytraverse to an actual linear lien
def expand_ytraverse(ytraverse,xmin,xmax):
	newlist =[]
	print ytraverse
	for row in ytraverse:
		point1 = [xmin,row]
		point2 = [xmax,row]
		newlist.append([point1,point2])
	return newlist

# from the first point found of a line returns the secocnd point
def get_closest_point(point,data):
	data['DIST'] = ((point[0]-data['LONG'])**2+(point[1]-data['LAT'])**2)**.5
	data = data.sort(['DIST'],ascending=[1])
	data = data[:5]
	data = data[data.LAT > point[1]] # knowing the point were looking for has to be above the datum
	data = data[:1]
	data = df2list(data)
	point = getlatlong(data[0],data[1])
	return point

def get_intersection(point1,point2,y):
	points = generate_points(point1,point2,30)
	print points


def get_points_horizontal(horizontal_line,data):
	# getting y 
	y = horizontal_line[0]
	y = y[1]

	temp = data[(data.LAT>y)|(data.LAT<y)]
	minimum = temp['LONG'].min()

	point = temp[temp.LONG == minimum]
	point = df2list(point)

	point1 = getlatlong(point[0],point[1])
	point2 = get_closest_point(point1,data)
	#data = bl.list2df(data)
	data['LONG'] = data['LONG'].round(9)
	data['LAT'] = data['LAT'].round(9)

	point2 = [['LONG','LAT'],point2]
	point2 = getlatlong(point2[0],point2[1])



	return point2


def get_points_ytraverse(horizontal_lines,data):
	print horizontal_lines
	for row in horizontal_lines:
		point = get_points_horizontal(row,data)
		print point


# point is a x,y list of a point
# dim is either string lat or long 
# sign is stirng of plus or minus
# data is the intial points df
# value of dimmension you wish to compare against
def get_closest_dim(datum,dim,sign,data):
	if dim == 'LAT':
		data['DELTA'] = data['LAT'] - datum
		if sign == 'plus':
			data = data[data['DELTA'] > 0]
		elif sign == 'minus':
			data = data[data['DELTA'] < 0]  
	elif dim == 'LONG':
		data['DELTA'] = data['LAT'] - datum
		if sign == 'plus':
			data = data[data['DELTA'] > 0]
		elif sign == 'minus':
			data = data[data['DELTA'] < 0]  
	data['DELTA'] = data['DELTA'].abs()
	data = data.sort(['DELTA'],ascending=[0])
	data = data[:1]
	data = df2list(data)

	point = getlatlong(data)

# given a dataframe returns a polygon alignment in dataframe form
def make_polygon_bypoints(data,delta):

	for row in data.columns.values.tolist():
		if 'lat' in str(row).lower():
			latheader = row
		if 'lon' in str(row).lower():
			longheader = row
	data['LAT'] = data[latheader]
	data['LONG'] = data[longheader]
	count = 0
	polygon = []
	yvalues = [0]
	max_yrow = data[data['LAT']==data['LAT'].max()]
	max_yrow = df2list(max_yrow)
	min_yrow = data[data['LAT']==data['LAT'].min()]
	min_yrow = df2list(min_yrow)
	min_yrow = getlatlong(min_yrow[0],min_yrow[1])
	max_yrow = getlatlong(max_yrow[0],max_yrow[1])


	extremalist = [['LONG','LAT'],min_yrow,max_yrow]
	extrema = get_extrema(data)
	yvalues = get_ytraverse(extrema['s'],extrema['n'],delta)
	count2 = 0
	leftside = [['LONG','LAT']]
	rightside = [['LONG','LAT']]
	ind = 0
	for row in yvalues:
		if count2 == 0 or ind == 1:
			count2 = 1
		else:
			print row,oldrow
			potentialvalues = data[(data['LAT'] < row)&(data['LAT'] > oldrow)]
			max_xrow = potentialvalues[potentialvalues['LONG']==potentialvalues['LONG'].max()]
			max_xrow = df2list(max_xrow)
			min_xrow = potentialvalues[potentialvalues['LONG']==potentialvalues['LONG'].min()]
			min_xrow = df2list(min_xrow)
			if not len(min_xrow) == 1:
				leftside.append(getlatlong(min_xrow[0],min_xrow[1]))
			if not len(max_xrow) == 1:
				rightside.append(getlatlong(max_xrow[0],max_xrow[1]))

		oldrow = row


		polygon = [['LONG','LAT'],min_yrow]
		for row in leftside[1:]:
			polygon.append(row)
		polygon.append(max_yrow)
		for row in reversed(rightside[1:]):
			polygon.append(row)
		polygon.append(min_yrow)
		print len(polygon)
	count += 1
	return polygon

'''
max_yrow = data[data['LAT']==data['LAT'].max()]
max_yrow = df2list(max_yrow)
min_yrow = data[data['LAT']==data['LAT'].min()]
min_yrow = df2list(min_yrow)
min_yrow = getlatlong(min_yrow[0],min_yrow[1])
max_yrow = getlatlong(max_yrow[0],max_yrow[1])


extremalist = [['LONG','LAT'],min_yrow,max_yrow]
print extremalist
extrema = get_extrema(data)
delta = .0001
yvalues = get_ytraverse(extrema['s'],extrema['n'],delta)
count = 0
leftside = [['LONG','LAT']]
rightside = [['LONG','LAT']]
for row in yvalues:
	if count == 0:
		count = 1
	else:
		print row,oldrow
		potentialvalues = data[(data['LAT'] < row)&(data['LAT'] > oldrow)]
		max_xrow = potentialvalues[potentialvalues['LONG']==potentialvalues['LONG'].max()]
		max_xrow = df2list(max_xrow)
		rightside.append(getlatlong(max_xrow[0],max_xrow[1]))
		min_xrow = potentialvalues[potentialvalues['LONG']==potentialvalues['LONG'].min()]
		min_xrow = df2list(min_xrow)
		leftside.append(getlatlong(min_xrow[0],min_xrow[1]))
	oldrow = row
polygon = [['LONG','LAT'],min_yrow]
for row in leftside[1:]:
	polygon.append(row)
polygon.append(max_yrow)
for row in reversed(rightside[1:]):
	polygon.append(row)
polygon.append(min_yrow)
'''
#bl.make_points(rightside,list=True,filename='rightside.geojson')
#bl.make_points(leftside,list=True,filename='leftside.geojson')
#bl.make_points(extremalist,list=True,filename='extrema.geojson')
bl.make_polygon(polygon,list=True,filename='polygon.geojson')
filedict = {'extrema.geojson':'red'}

bl.loadparsehtml(bl.collect(),True,file_dictionary=filedict)