import geohash 
from geohash_logic import distance
import pandas as pd
import numpy as np

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

# gets the positons of based on signs of a geohash
def get_position(ghash,xsign,ysign):
	y,x,ydelta,xdelta = geohash.decode_exactly(ghash)
	if xsign == '-':
		x = x - xdelta
	if xsign == '+':
		x = x + xdelta
	if ysign == '-':
		y = y - ydelta
	if ysign == '+':
		y = y + ydelta
	return x,y

# gets both positions a get_postion rapper
def get_both_positions(ghash):
	x1,y1 = get_position(ghash,'-','+')
	x2,y2 = get_position(ghash,'-','-')
	return y1,y2,x1

# gets the corners in a dataframe
def get_corners(neighbors):
	nw,ne,sw,se = [6,7,3,4]
	nw,ne,sw,se = neighbors[nw],neighbors[ne],neighbors[sw],neighbors[se]
	d = pd.DataFrame([nw,ne,sw,se],columns=['GEOHASH'])
	d['R'] = range(len(d))
	return d

# gets the corner points
def get_corner_points(neighbors):
	nw,ne,sw,se = [6,7,3,4]
	nw,ne,sw,se = neighbors[nw],neighbors[ne],neighbors[sw],neighbors[se]
	y1,y2,x = get_both_positions(nw)
	y3,y4,x = get_both_positions(sw)

	return y1,y2,y3,y4,x

# makes points x,y list for each y value
def make_points(y1,y2,y3,y4,x,df=False):
	ys = [y1,y2,y3,y4]
	xs = [x] * 4
	if df == False:
		return zip(xs,ys)
	else:
		return pd.DataFrame(zip(xs,ys),columns=['LONG','LAT'])

# makes points out of the 4 corner points
def make_df(neighbors):
	a,b,c,d,e = get_corner_points(neighbors)
	return make_points(a,b,c,d,e,df=True)	

# gets a slope
def get_slope(pt1,pt2):
	x1,y1 = pt1
	x2,y2 = pt2

	if x1 == x2: 
		slope = 10000000
	else:
		slope = (float(y2) - float(y1)) / (float(x2) - float(x1))
	
	return slope

# interpolation
def interpol_point(xmin,slope,pt1):
	y = - (slope * (pt1[0] - xmin)) + pt1[1]
	return y

# solves and returns the positions of each neighbor function that will be called
def solve_xmin(pt1,pt2,size):
	positiondict = {'neg':[4,6,2,5,0,1],'pos':[3,7,2,5,0,1],'vert':[2,5],'zero':[0,1]}
	
	ghash = geohash.encode(pt1[1],pt1[0],size)
	
	# getting neighbors
	neighbors = geohash.neighbors(ghash)

	# setting up variables for slope determination
	y1,y2,y3,y4,xmin = get_corner_points(neighbors)

	# getting slope
	slope = get_slope(pt1,pt2)

	y = interpol_point(xmin,slope,pt1)
	if y < y1 and y > y2:
		slope = 'neg'
		tang = 'pos'
	elif y < y2 and y > y3:
		slope = 'zero'
		tang = 'vert'
	elif y < y3 and y > y4:
		slope = 'pos'
		tang = 'neg'
	elif y >= y1 or y <= y4:
		slope = 'vert'
		tang = 'zero'
	if tang == 'pos' or tang == 'neg':
		pos1,pos2,pos3,pos4,pos5,pos6 = positiondict[tang]
		return [pos1,pos2,pos3,pos4,pos5,pos6]
	pos1,pos2 = positiondict[tang]
	return [pos1,pos2]

# gets the diagonal hash size
def get_hashsize(ul,size):
	# getting geohash for ul and lr
	# assuming 8 for the time being as abs min
	ulhash = geohash.encode(ul[1],ul[0],size)

	lat,long,latdelta,longdelta = geohash.decode_exactly(ulhash)

	latdelta,longdelta = latdelta * 2.0,longdelta * 2.0

	hashsize = ((latdelta ** 2) + (longdelta ** 2)) ** .5

	return hashsize

# making flat list non sorted
def flatten_nonsorted(data):
	indexes = np.unique(data, return_index=True)[1]
	list = [data[index] for index in sorted(indexes)]
	return list	

# generates the tang geohashs basedon positions
def generate_tangs(geohashlist,positions):
	geohashlist = [i[:-1] for i in geohashlist]
	geohashlist = flatten_nonsorted(geohashlist)
	geohashlist = [geohash.neighbors(i) for i in geohashlist]
	if len(positions) == 6:
		geohashlist = [[i[positions[0]],i[positions[1]],i[positions[2]],i[positions[3]],i[positions[4]],i[positions[5]]] for i in geohashlist]
	else:
		geohashlist = [[i[positions[0]],i[positions[1]]] for i in geohashlist]
	return sum(geohashlist,[])

# generates ghashs between points and ghash tangs about a set of poitns
# main function
def geohash_delta(pt1,pt2,size,hashsize):
	positions = solve_xmin(pt1,pt2,size)
	name = ''

	oldrow,row = pt1,pt2
	geohashlist = []
	dist = distance(oldrow,row)
	if dist > hashsize / 5.0:
		number = (dist / hashsize) * 5.0
		number = int(number)
		geohashlist += generate_points_geohash(number,oldrow,row,name,size)[1:]
	else:
		point = row[:-1]
		geohashlist.append(geohash.encode(point[1],point[0],size))

	ghashs = flatten_nonsorted(geohashlist)
	tangs = generate_tangs(ghashs,positions)
	return ghashs,np.unique(tangs).tolist()


# makes just the flattened geohashs
def geohash_ghash(pt1,pt2,size,hashsize):
	positions = solve_xmin(pt1,pt2,size)
	name = ''

	oldrow,row = pt1,pt2
	geohashlist = []
	dist = distance(oldrow,row)
	if dist > hashsize / 5.0:
		number = (dist / hashsize) * 5.0
		number = int(number)
		geohashlist += generate_points_geohash(number,oldrow,row,name,size)[1:]
	else:
		point = row[:-1]
		geohashlist.append(geohash.encode(point[1],point[0],size))

	ghashs = flatten_nonsorted(geohashlist)
	return ghashs