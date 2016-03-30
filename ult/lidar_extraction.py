import easylidar as el
import berrl as bl
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pipekml.points import *
import itertools

# stringify a list that may come out as integers but is really a string within the dataframe
def stringify(list):
	newlist=[]
	for row in list:
		newlist.append(str(row))
	return newlist



# making trimmed lines and returning sliced geohahss
def make_line_parts(precision,routes,data):
	# where routes is the route database
	# lineframe is the created lineframe from routes
	# making lineframe
	lineframe = bl.make_line_frame(routes,precision,1,csv=True)
	newlineframe = bl.querry_multiple(lineframe,'GEOHASH',np.unique(data['GEOHASH']).tolist())

	# getting unique routes
	uniqueroutes = np.unique(newlineframe['routeid']).tolist()

	# getting unique routes from post gis dataframe
	routesincloud = bl.querry_multiple(routes,'routeid',stringify(uniqueroutes))
	routesincloud = routesincloud[routesincloud.signsystem == '1']

	# getting header for postgis table iteration
	header = bl.df2list(routesincloud)[0]

	# postgis table integration
	count=0
	uniquegeohashs=[]
	for row in bl.df2list(routesincloud)[1:]:
		count+=1
		temp = bl.extend_geohashed_table(header,row,7,return_dataframe=True)
		temp = bl.querry_multiple(temp,'GEOHASH',np.unique(data['GEOHASH']).tolist())
		uniquegeohashs += np.unique(temp['GEOHASH']).tolist()
		bl.make_line(temp,list=True,filename=str(count)+'.geojson')
	return uniquegeohashs


# getting dataframes
def get_partial_dataframes(precision,routes,data):
	# where routes is the route database
	# lineframe is the created lineframe from routes
	# making lineframe
	lineframe = bl.make_line_frame(routes,precision,1,csv=True)
	newlineframe = bl.querry_multiple(lineframe,'GEOHASH',np.unique(data['GEOHASH']).tolist())

	# getting unique routes
	uniqueroutes = np.unique(newlineframe['routeid']).tolist()

	# getting unique routes from post gis dataframe
	routesincloud = bl.querry_multiple(routes,'routeid',stringify(uniqueroutes))
	routesincloud = routesincloud[routesincloud.signsystem == '1']

	# getting header for postgis table iteration
	header = bl.df2list(routesincloud)[0]

	# postgis table integration
	count=0
	newlist=[]
	for row in bl.df2list(routesincloud)[1:]:
		count+=1
		temp = bl.extend_geohashed_table(header,row,7,return_dataframe=True)
		temp = bl.querry_multiple(temp,'GEOHASH',np.unique(data['GEOHASH']).tolist())
		newlist.append(temp)
	return newlist

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

# given two points gets linear line between the two 
# returns slope and y intercept
def get_linear_line_vars(oldpoint,point):
	slope = (point[1] - oldpoint[1]) / (point[0] - oldpoint[0])
	b = point[1] - slope*point[0]
	return [slope,b]

# from the output of get_linear_line_vars() and the current point returns perpendicular line vars
def get_perpendicular_line(point,vars):
	slope = (vars[0]**-1)*-1
	b = point[1] - point[0]*slope
	return [slope,b]


# returns a line frame given vars a vars output from 
def return_line_frame(vars,xvalues):
	slope = vars[0]
	b = vars[1]
	newlist = [['LONG','LAT']]
	for row in xvalues:
		y = (float(row)*slope) + b
		newlist.append([row,y])
	return bl.list2df(newlist)

# given a rounding precision a starting x and ending x returns a list of lines inbetween
def make_x_points_rounding(rounding,start,end):
	delta = 10**-rounding
	print delta
	current = float(start)
	xvalues = []
	while current < end:
		current += delta
		xvalues.append(current)
	return xvalues

def make_x_points(number_of_points,start,end):
	totaldelta = float(end) - float(start)
	delta = totaldelta / float(number_of_points)
	current = float(start)
	xvalues = []
	count = 0
	while count < number_of_points:
		count+=1
		current += delta
		xvalues.append(current)
	return xvalues

# returns minimum point of a dataframe
def return_minimum_point(point,clouddata):
	clouddata['DIST'] = ((clouddata['LAT']-point[1])**2 + (clouddata['LONG']-point[0])**2)**.5
	clouddata = clouddata[clouddata['DIST'] == clouddata['DIST'].min()]
	return clouddata.values.tolist()[0]

# given a table of lat, longs representing a perp line returns a list of closest lidar data corresponding to that
def return_min_table(pointtable,pointdata):
	pointtable = bl.df2list(pointtable)
	header = bl.df2list(pointdata)[0]
	add_dist=True
	for row in header:
		if row=='DIST':
			add_dist=False
	if add_dist==True:
		header = bl.df2list(pointdata)[0]+['DIST']
	newlist = [header]
	count=0
	totalcount=0
	for row in pointtable[1:]:
		count+=1
		newrow = return_minimum_point(row,pointdata)
		newlist.append(newrow)

		if count==1000:
			count=0
			totalcount+=1000
			print totalcount

	newnewlist = [header]
	unique=[]
	for row in newlist:
		ind=0
		oldrow=row
		for row in unique:
			if oldrow==row:
				ind=1
		if ind==0:
			newnewlist.append(oldrow)
			unique.append(oldrow)
	return bl.list2df(newnewlist)

# given a table of lat, longs representing a perp line returns a list of closest lidar data corresponding to that
def return_distance_table(point,perpendicular_points):
	perpendicular_points['DIST2'] = (((perpendicular_points['LAT']-point[1])**2 + (perpendicular_points['LONG']-point[0])**2)**.5)*69.2*5280
	return perpendicular_points

# define scope of rounding about point along alignment
def return_point_scope(point,perpendicular_points,feet):
	data = return_distance_table(point,perpendicular_points)
	perpendicular_points = data[data.DIST2 < feet]
	return perpendicular_points

# extends out distance 
def return_cross_distance(perpendicular_points):
	perpendicular_points = bl.df2list(perpendicular_points)
	newlist = [perpendicular_points[0]+['DIST_PERP']]
	header = perpendicular_points[0]+['DIST_PERP']
	intial_point = getlatlong(perpendicular_points[0],perpendicular_points[1])
	for row in perpendicular_points[1:]:
		point = getlatlong(header,row)
		distance = ((point[1]-intial_point[1])**2 + (point[0]-intial_point[0])**2)**.5*69.2*5280
		newrow = row + [distance]
		newlist.append(newrow)
	return bl.list2df(newlist)

# returns the end points of each perpendicular line
def get_ends(perpendicular_points):
	perpendicular_points = bl.df2list(perpendicular_points)
	header = perpendicular_points[0]
	start = perpendicular_points[1]
	end = perpendicular_points[-1]
	newlist = [header,start,end]
	return newlist

# given perpvars and a feet will add and subtract the perpendicular linear distance
def generate_perpendicular_extrema(perpvars,point,feet):
	# setting up delta
	delta = float(feet)/float(5280.0)/float(62.9)

	# setting up x values
	x1 = point[0]-delta
	x2 = point[0]+delta

	# extreama 1 calculation
	extremay1 = x1*perpvars[0]+perpvars[1]
	extremay2 = x2*perpvars[0]+perpvars[1]

	if extremay1 > extremay2:
		top= [x1,extremay1]
		bottom = [x2,extremay2]
	elif extremay2 >= extremay1:
		top = [x2,extremay2]
		bottom = [x1,extremay1]


	return [['LONG','LAT'],[x1,extremay1],[x2,extremay2]]

# given perpvars and a feet will add and subtract the perpendicular linear distance
def generate_push_point(perpvars,point,feet):
	# setting up delta
	delta = float(feet)/float(5280.0)/float(62.9)

	# setting up x values
	x1 = point[0]+delta
	x2 = point[0]+delta

	# extreama 1 calculation
	extremay1 = x1*perpvars[0]+perpvars[1]
	extremay2 = x2*perpvars[0]+perpvars[1]

	if perpvars[0]>0:
		return [x1,extremay1]
	else:
		return [x2,extremay2]

# creates a roadway alignment from any roadway frame
def create_df_alignment(pushpoint,roadwayframe,feet):
	a = roadwayframe

	if isinstance(a,pd.DataFrame):
		a = bl.df2list(a)

	header=a[0]
	count=0
	count2=0
	count3=0
	oldpoint=pushpoint
	newlist = [['LONG','LAT']]
	for row in a[2:]:
		# getting point
		point = getlatlong(header,row)
		vars = get_linear_line_vars(oldpoint,point)
		perpvars = get_perpendicular_line(point,vars)
		point = [point[0],vars[0]*point[0]+vars[1]]
		point = generate_push_point(perpvars,point,feet)

		newlist.append(point)
		oldpoint = point
	return newlist


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

# gets definitive y maximum value
def get_ymax(toppoints,bottompoints):
	# getting maxpoint currently available
	ymax1 = toppoints[1]
	ymax2 = bottompoints[1]
	if ymax1 >= ymax2:
		ymax = ymax1
	elif ymax2 > ymax1:
		ymax = ymax2

	return ymax

# gets definitive y minimum value
def get_ymin(toppoints,bottompoints):
	# getting minpoint currently available
	ymin1 = toppoints[1]
	ymin2 = bottompoints[1]
	if ymin1 <= ymin2:
		ymin = ymin1
	elif ymin2 < ymin1:
		ymin = ymin2

	return ymin

# gets definitive x min value
def get_xmax(toppoints,bottompoints):
	# getting maxpoint currently available
	xmax1 = toppoints[0]
	xmax2 = bottompoints[0]
	if xmax1 >= xmax2:
		xmax = xmax1
	elif xmax2 > xmax1:
		xmax = xmax2

	return xmax

# gets definitive x maximum value
def get_xmin(toppoints,bottompoints):
	# getting minpoint currently available
	xmin1 = toppoints[0]
	xmin2 = bottompoints[0]
	if xmin1 <= xmin2:
		xmin = xmin1
	elif xmin2 < xmin1:
		xmin = xmin2

	return xmin


# getting temporary cloud data
def get_temp_cloud(cloud,ymax,ymin,xmax,xmin):
	tempcloud = cloud[(cloud.LAT < ymax)&(cloud.LAT > ymin)&(cloud.LONG < xmax)&(cloud.LONG > xmin)]
	return tempcloud

# creates a window for an alignment to be within
def create_window(table,feet):
	table=bl.df2list(table)
	header=table[0]
	newlist=[header]
	oldrow = table[1]
	oldpoint = getlatlong(header,oldrow)
	totalpoints=[['LONG','LAT','LONG2','LAT2']]
	for row in table[2:]:
		point = getlatlong(header,row)
		vars = get_linear_line_vars(oldpoint,point)
		perpvars = get_perpendicular_line(point,vars)
		newpoints = generate_perpendicular_extrema(perpvars,point,feet)
		newrow=newpoints[1]+newpoints[2]
		totalpoints.append(newrow)
		oldpoint = point
		oldrow =row
	return bl.list2df(totalpoints)

# creates xslides to window through when iterating through each point
# this will take each point and interoplate lots ofpoints in between each
def create_window_slides(table):
	# seperating tables
	table1 = table[['LAT','LONG']]
	table2 = table[['LAT2','LONG2']]

	# to list
	table1 = bl.df2list(table1)
	table2 = bl.df2list(table2)
	apointstotal=[['LAT','LONG']]
	bpointstotal=[['LAT','LONG']]
	count=0
	for a,b in itertools.izip(table1[1:],table2[1:]):
		if count==0:
			count=1
		else:
			apoints = generate_points(100,a,olda)
			bpoints = generate_points(100,b,oldb)

			# total of each point side
			apointstotal+=apoints[1:]
			bpointstotal+=bpoints[1:]

		olda=a
		oldb=b

	return [apointstotal,bpointstotal]

# from the output of the function above creates df from boundry points if applicable
def slide_through(tables,cloud):
	# getting tables
	table1 = tables[0]
	table2 = tables[1]
	header = cloud.columns.values.tolist()
	newlist = [header]
	for a,b in itertools.izip(table1[1:],table2[1:]):
		x1 = a[1] + float(1)/float(5280)/float(69.2)
		x2 = a[1] - float(1)/float(5280)/float(69.2)

		y1 = a[0]
		y2 = b[0]

		temp = cloud[(cloud.LONG>x2)&(cloud.LONG<=x1)&(cloud.LAT>y1)&(cloud.LAT<=y2)]
		if not len(temp) == 0:
			temp = bl.df2list(temp)
			newlist.append(temp[1])
	return newlist

def make_point_cloud(roadwayframe,cloud):
	a = roadwayframe

	if isinstance(a,pd.DataFrame):
		a = bl.df2list(a)

	header=a[0]
	count=0
	count2=0
	count3=0
	for row in a[1:]:
		# getting point
		point = getlatlong(header,row)
		if count == 0:
			count = 1
		else:
			vars = get_linear_line_vars(oldpoint,point)
			perpvars = get_perpendicular_line(point,vars)
			
			# generating perpendicular end
			extrema = generate_perpendicular_extrema(perpvars,point,20)

			# getting top and bottum point
			toppoints = getlatlong(extrema[0],extrema[1])
			bottompoints = getlatlong(extrema[0],extrema[2])
			print extrema

			if count2 == 0:
				count2=1
				ymax = -180
				ymin = float(90)
				xmax = -180
				xmin = 180
			else:
				ymax = -180
				ymin = float(90)
				xmax = -180
				xmin = 180
				count+=1
				ymax = get_ymax(toppoints,bottompoints)
				ymin = get_ymin(toppoints,bottompoints)
				xmax = get_xmax(toppoints,bottompoints)
				xmin = get_xmin(toppoints,bottompoints)

				# generate_point rating top and bottom points
				topgeneratedpoints = generate_points(100,oldtoppoints,toppoints)
				bottomgeneratedpoints = generate_points(100,oldbottompoints,bottompoints)


				tempcloud = get_temp_cloud(cloud,ymax,ymin,xmax,xmin)
				
				if not len(tempcloud) == 0:
					'''
					for a,b in itertools.izip(topgeneratedpoints[1:],bottomgeneratedpoints[1:]):
						newpoints = generate_points(100,a,b)
						bulkpoints += newpoints[1:]
					'''
					#colors = ['blue','light blue','light green','yellow','red']
					#ranges = [0,50,100,150,200,256]
					#filedict = bl.make_object_map(tempcloud,'INTENSITY',ranges,colors,'points')
					tempcloud = tempcloud[(tempcloud.INTENSITY>200)&(tempcloud.INTENSITY<=256)]
					if count3==0:
						count3=1
						maincloud=tempcloud
					else:
						maincloud = bl.concatenate_tables(maincloud,tempcloud)

					a=make_points(tempcloud,list=True)
			oldtoppoints = toppoints
			oldbottompoints = bottompoints
		oldpoint = point
	return maincloud

# building from a route tree
def make_boundry_check(maincloud,dfalignment):
	# creating line points
	window = create_window(dfalignment,.1)

	# creating line points in x
	window = create_window_slides(window)

	# creating table containing points within boundry
	dfoutput = slide_through(window,maincloud)
	return dfoutput

# getting first perpvars
def get_first_perpvars(roadwayframe):
	a = roadwayframe

	if isinstance(a,pd.DataFrame):
		a = bl.df2list(a)

	header = a[0]
	point1 = getlatlong(header,a[1])
	point2 = getlatlong(header,a[2])
	vars = get_linear_line_vars(point1,point2)
	perpvars = get_perpendicular_line(point2,vars)

	return [perpvars,point2]


# attempts a whole alignment sequence to see if any points fall wihtin route
def try_alignment(perpvars,point,dataframe,cloudfilter,feet):
	# where perpvars is perpendicular line mx+b
	# where point is the point to start at
	# dataframeis the alignment frame in question
	# cloud filter is point data were trying to grab
	# feet is the amound in which the point is displaced perpendicularrrly


	# generating a tangential pont
	pushpoint = generate_push_point(perpvars,point,float(feet))

	# create roadway alignment
	alignment = create_df_alignment(pushpoint,dataframe,float(feet))

	# making boundry check about alignment
	boundry = make_boundry_check(cloudfilter,bl.list2df(alignment))

	return boundry
#
# attempts a whole alignment sequence to see if any points fall wihtin route
def create_alignment(perpvars,point,dataframe,cloudfilter,feet):
	# where perpvars is perpendicular line mx+b
	# where point is the point to start at
	# dataframeis the alignment frame in question
	# cloud filter is point data were trying to grab
	# feet is the amound in which the point is displaced perpendicularrrly


	# generating a tangential pont
	pushpoint = generate_push_point(perpvars,point,float(feet))

	# create roadway alignment
	alignment = create_df_alignment(pushpoint,dataframe,float(feet))

	return alignment

def find_alignments(perpvars,point,dataframe,cloudfilter):
	current=0
	newlist=[['routeid','FOUND','POSITION']]
	while current<30:
		current+=.25
		current1=current
		current2=current*-1
		data2=try_alignment(first,point,a,cloudfilter,current1)
		data2=data2[1:]
		data3=try_alignment(first,point,a,cloudfilter,current2)
		data3=data3[1:]
		print current,len(data2),len(data3)
		for row in data2:
			row=[row[0],len(data2),current1]
			newlist.append(row)
		for row in data3:
			row=[row[0],len(data3),current2]
			newlist.append(row)
	return newlist

def pepper_alignments(alignment):
	count=0
	print 'ads',alignment
	if isinstance(alignment,pd.DataFrame):
		alignment = bl.df2list(alignment)
	
	header = alignment[0]
	newpoints = [['LONG','LAT']]
	for row in alignment[1:]:
		point = getlatlong(header,row)
		if count == 0:
			count=1
		else:
			newpoints += generate_points(100,point,oldpoint)[1:]
		oldrow=row
		oldpoint=point
	return newpoints



a=pd.read_csv('1.csv')
cloud = pd.read_csv('cloud.csv')

# making point cloud
cloudfilter = make_point_cloud(a,cloud)

# getting first perpvars
first = get_first_perpvars(a)
point = first[1]
first=first[0]
key='pk.eyJ1IjoibXVycGh5MjE0IiwiYSI6ImNpam5kb3puZzAwZ2l0aG01ZW1uMTRjbnoifQ.5Znb4MArp7v3Wwrn6WFE6A'


alignment1 = create_alignment(first,point,a,cloudfilter,-.25)
alignment2 = create_alignment(first,point,a,cloudfilter,-5)

alignmentfull1 = pepper_alignments(alignment1)
alignmentfull2 = pepper_alignments(alignment2)

alignmentfinal = [['LONG','LAT']]
print alignment2[:20]
for a,b in itertools.izip(alignmentfull1[1:],alignmentfull2[1:]):
	points = generate_points(40,a,b)
	alignmentfinal += points
alignmentfinal = bl.list2df(alignmentfinal[1:])
alignmentfinal['BOOL'] = alignmentfinal['LONG'].isin(['LONG'])
alignmentfinal = alignmentfinal[alignmentfinal.BOOL == False]
alignmentfinal['BOOL'] = alignmentfinal['LAT'].isin(['LAT'])
alignmentfinal = alignmentfinal[alignmentfinal.BOOL == False]
alignmentfinal = alignmentfinal[['LONG','LAT']]
alignmentfinal[['LONG','LAT']]=alignmentfinal[['LONG','LAT']].astype(float)

# taking the maximum value of alignment final to querry out uneeded values
maximum = alignmentfinal['LAT'].max()
print len(cloud)
cloud = cloud[cloud.LAT<maximum]
print len(cloud)
lidar_data = return_min_table(alignmentfinal,cloud)
lidar_data.to_csv('new_lidar_out.csv')
print lidar_data
parselist(make_points(alignment2,list=True),'a.kml')
parselist(make_points(alignment1,list=True),'b.kml')


'''
data = find_alignments(first,point,a,cloudfilter)
bl.writecsv(data,'output.csv')

parselist(make_points(alignment,list=True),'a.kml')
parselist(make_points([['LONG', 'LAT'],pushpoint],list=True),'b.kml')

parselist(make_points(boundry,list=True),'c.kml')
#parselist(make_points(make_boundry_check(cloudfilter,a),list=True),'ee.kml')

points=create_window(a,.25)
c=create_window_slides(points)
bl.make_line(points[['LAT','LONG']],list=True,filename='line.geojson')
bl.make_line(points[['LAT2','LONG2']],list=True,filename='line2.geojson')

parselist(make_points(points[['LAT','LONG']],list=True),'a.kml')
parselist(make_points(points[['LAT2','LONG2']],list=True),'b.kml')
parselist(make_points(c[0],list=True),'c.kml')
parselist(make_points(c[1],list=True),'d.kml')



# analyze point lidar data against alignment iteration going to find first alignment points
# i.e. finding first roadway bed points
count=0
minimum = a['LONG'].min()
maximum = a['LONG'].max()
a=bl.df2list(a)

data = make_point_cloud(a,cloud)

header=a[0]

count2=0
count3=0
for row in a[20:34]:
	# getting point
	point = getlatlong(header,row)
	if count == 0:
		count = 1
	else:
		vars = get_linear_line_vars(oldpoint,point)
		perpvars = get_perpendicular_line(point,vars)
		
		# generating perpendicular end
		extrema = generate_perpendicular_extrema(perpvars,point,20)

		# getting top and bottum point
		toppoints = getlatlong(extrema[0],extrema[1])
		bottompoints = getlatlong(extrema[0],extrema[2])
		print extrema

		if count2 == 0:
			count2=1
			ymax = -180
			ymin = float(90)
			xmax = -180
			xmin = 180
		else:
			ymax = -180
			ymin = float(90)
			xmax = -180
			xmin = 180
			count+=1
			ymax = get_ymax(toppoints,bottompoints)
			ymin = get_ymin(toppoints,bottompoints)
			xmax = get_xmax(toppoints,bottompoints)
			xmin = get_xmin(toppoints,bottompoints)

			# generate_point rating top and bottom points
			topgeneratedpoints = generate_points(100,oldtoppoints,toppoints)
			bottomgeneratedpoints = generate_points(100,oldbottompoints,bottompoints)


			tempcloud = get_temp_cloud(cloud,ymax,ymin,xmax,xmin)
			
			if not len(tempcloud) == 0:
				for a,b in itertools.izip(topgeneratedpoints[1:],bottomgeneratedpoints[1:]):
					newpoints = generate_points(100,a,b)
					bulkpoints += newpoints[1:]
				#colors = ['blue','light blue','light green','yellow','red']
				#ranges = [0,50,100,150,200,256]
				#filedict = bl.make_object_map(tempcloud,'INTENSITY',ranges,colors,'points')
				tempcloud = tempcloud[(tempcloud.INTENSITY>200)&(tempcloud.INTENSITY<=256)]
				if count3==0:
					count3=1
					maincloud=tempcloud
				else:
					maincloud = bl.concatenate_tables(maincloud,tempcloud)

				a=make_points(tempcloud,list=True)
				parselist(a,'points'+str(count)+'.kml')	
		oldtoppoints = toppoints
		oldbottompoints = bottompoints
	oldpoint = point

bl.make_points(maincloud,list=True,filename='points.geojson')

a = slide_through(c,maincloud)
parselist(make_points(a,list=True),'e.kml')

bl.loadparsehtml(bl.collect(),key)

for row in a[20:22]:
	print row
	point = getlatlong(header,row)
	if count==0:
		count=1
	else:
		count+=1
		vars = get_linear_line_vars(oldpoint,point)
		perpvars = get_perpendicular_line(point,vars)
		line = return_line_frame(perpvars,[point[0],maximum])
		line2 = return_line_frame(vars,[minimum,point[0]])
		
		# generating x values
		xvalues = make_x_points_rounding(6,minimum,maximum)

		# getting line of simulated perp line
		linepoints = return_line_frame(perpvars,xvalues)
		
		# getting minimum points
		perpendicular_points = return_min_table(linepoints,cloud)

		# querry close to current point
		perpendicular_points = return_point_scope(point,perpendicular_points,30)
		
		print '30 feet'
		if not len(perpendicular_points) == 0:
			# extend out point distance
			perpendicular_points = return_cross_distance(perpendicular_points)
			
			# saving points dataframe so it can be put on the plot
			old_perpendicular_points = perpendicular_points

			# grabbing each end point
			perpendicular_points = get_ends(perpendicular_points)

			# start x 
			startx = getlatlong(perpendicular_points[0],perpendicular_points[1])[0]

			# make endx
			endx = getlatlong(perpendicular_points[0],perpendicular_points[2])[0]

			# make points
			newxvalues = make_x_points(100,startx,endx)

			# getting line of simulated perp line
			newlinepoints = return_line_frame(perpvars,newxvalues)

			# getting ends from newlinepoints
			ends = get_ends(newlinepoints)

			if count2 == 0:
				oldlinepoints = newlinepoints
				oldends = ends
			else:
				# getting top and bottum 
				topstart,topend = ends[0],oldends[0]
				bottomstart,bottomend = ends[1],oldends[1]

				# getting x values
				topxvalues = make_x_points(100,topstart[0],topend[0])
				bottumxvalues = make_x_points(100,bottomstart[0],bottomend[0])

				toplinepoints = return_line_frame(perpvars,newxvalues)


			bl.make_points(linepoints,list=True,filename='perp_points'+str(count)+'.geojson')


			bl.make_line(line,list=True,filename='line.geojson')
			bl.make_line(line2,list=True,filename='line2.geojson')
			plt.plot(old_perpendicular_points['DIST_PERP'],old_perpendicular_points['ELEVATION'])
	oldpoint = point


plt.xlim([0, old_perpendicular_points['DIST_PERP'].max()])
plt.ylim([old_perpendicular_points['ELEVATION'].min()-10, old_perpendicular_points['ELEVATION'].max()+10])
plt.title('Lidar-Data Alignment Cross Section')
plt.xlabel('Distance From First Point')
plt.ylabel('Elevation (ft)') 








plt.show()
bl.loadparsehtml(bl.collect(),key)
'''