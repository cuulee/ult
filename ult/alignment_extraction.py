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

	return [xmin,xmax,ymin,ymax]
	
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
		ytraverse.append(current)
	return ytraverse

# expands out the values in the ytraverse to an actual linear lien
def expand_ytraverse(ytraverse,xmin,xmax):
	newlist =[]
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
	print point
	point1 = getlatlong(point[0],point[1])
	point2 = get_closest_point(point1,data)

	data = list2df(data)
	data['LONG'] = data['LONG'].round(9)
	data['LAT'] = data['LAT'].round(9)

	point2 = df2list(point2)
	print point2[1]
	point2 = getlatlong(point2[0],point2[1])

	return point2


def get_points_ytraverse(horizontal_lines,data):
	for row in horizontal_lines:
		point = get_points_horizontal(row,data)
		print point

data=pd.read_csv('lidar_csv.csv')

delta = 10**-5
extrema = get_extrema(data)
xmin = extrema[0]
xmax = extrema[1]
ymin = extrema[2]
ymax = extrema[3]


ytraverse = get_ytraverse(ymin,ymax,delta)
print ytraverse
horizontal_lines = expand_ytraverse(ytraverse,xmin,xmax)
print horizontal_lines
get_points_ytraverse(horizontal_lines,data)


