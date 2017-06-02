import itertools
import pandas as pd
import numpy as np
import geohash
#from quickmaps import *

# makes a base set of hash characters in no particular order
def make_hashs(hash):
	newlist = []
	for row in '0123456789bcdefghjkmnpqrstuvwxyz':
		row = str(hash)+str(row)

		newlist.append(row)
	return newlist

# a sub functure of assembling a geohash level
def first(list,stepcount):
	newlist = []
	count = 0
	totallist = []
	for row in list:
		count += 1 
		newrow = [[row[0],row[2]],[row[1],row[3]]]
		newlist.append(newrow)

		if count == stepcount:
			totallist.append(newlist)
			count = 0
			newlist = []

	if totallist == []:
		totallist = newlist			
	return totallist


# this function is used for representing EVEN level of geohash precisions
def make_hash_df8(**kwargs):
	indexdfs = False
	for key,value in kwargs.iteritems():
		if key == 'indexdfs':
			indexdfs = value

	count = 0
	newlist = []
	firsttotal = []
	for row in '0123456789bcdefghjkmnpqrstuvwxyz':
		newlist.append(row)
		count += 1
		if count == 4:
			firsttotal.append(newlist)
			newlist = []
			count = 0
	second_total = first(firsttotal,2)
	
	#second_total = first(total,2)
	totalone = []
	totaltwo = []
	for a,b,c,d in itertools.izip(second_total[0],second_total[1],second_total[2],second_total[3]):
		arow = a[0] + b[0]
		brow = a[1] + b[1]
		totalone.append(arow)
		totalone.append(brow)

		crow = c[0] + d[0]
		drow = c[1] + d[1]	
		totaltwo.append(crow)
		totaltwo.append(drow)

	total = totalone + totaltwo	
	newtotal = []
	for row in reversed(total):
		newtotal.append(row)

	if not indexdfs == False:
		return indexdfs[0]
	return pd.DataFrame(newtotal)

# a combine map function used for mapping about a df
def combine_two(each):
	global letter
	return letter + each

# reverses a df order for creating an opposite geohash level
# two base geohash levels exist: one for even percentages and another for odd 
def reverse_df(df):
	newlist = []
	totallist = []
	for row in reversed(df.index.values.tolist()):
		indexrow = df.loc[row]
		for row in reversed(indexrow.values.tolist()):
			newlist.append(row)
		totallist.append(newlist)
		newlist = []
	return pd.DataFrame(totallist)

# this function creates a base dataframe for odd level geohashs
def make_hash_df7():
	df = make_hash_df8().T
	return reverse_df(df)

# given a unique hash globally calls the prefix and 
# prepends the prefix to the hash and returns the strings
def addprefix(hash):
	global prefix

	return str(prefix) + str(hash)


# given the first x letters being the prefix prepends every geohash 
# sequence to the end of the prefix 
def add_prefix(prefix2,hashtable):
	global prefix
	prefix = prefix2
	hashtable = hashtable.applymap(addprefix)
	return hashtable

# given a unique hash globally calls the prefix and 
# prepends the prefix to the hash and returns the strings
def addpostfix(hash):
	global prefix

	return  str(hash) + str(prefix)


# given the first x letters being the prefix prepends every geohash 
# sequence to the end of the prefix 
def add_end(prefix2,hashtable):
	global prefix
	prefix = prefix2
	hashtable = hashtable.applymap(addpostfix)
	return hashtable

# given a hashtable, and a color position from the tail of each geohash
# returns a blocks table ready to be made into geojson with colorkeys 
# fields already added based on input colorposition
def color_table(hashtable,colorposition,**kwargs):
	geohashs = False
	for key,value in kwargs.iteritems():
		if key == 'geohashs':
			geohashs = value

	# assumes a hash table input intially
	if geohashs == False:
		# unstacking hashtable and turning into list
		hashs = hashtable.unstack(level=0).values.tolist()
	elif geohashs == True:
		hashs = hashtable

	# making blocks table
	blocktable = make_geohash_blocks(hashs)

	# making field for unique colorkey
	blocktable['string'] = blocktable.GEOHASH.str[-colorposition]

	# mapping unique blocks
	blocktable = unique_groupby(blocktable,'string')
	return blocktable

# the mapped function add hash uses to map a dataframe against a specific hash
def addhash2(innerhash):
	global hash1
	return hash1 + innerhash

# function used to generate the new dataframe with one appeneded hash
# this function is mapped and encapuslated within the add_hashtable function
def addhash(outerhash):
	global add_table
	global hash1
	hash1 = outerhash
	add = add_table.applymap(addhash2)
	return add

# given a hash table and the next count returns a 
# dataframe with the appropriate level of precision appeneded
def add_hashtable(hashtable,count):
	global add_table
	global hash1

	if float(count) / 2.0 == int(count) / 2:
		add_table = make_hash_df8()
	else:
		add_table = make_hash_df7()

	newlist = []
	newlist2 = []
	for row in hashtable.values.tolist():
		oldrow = row
		for row in oldrow:
			hash1 = row
			add = addhash(hash1)
			newlist.append(add)
		newlist = pd.concat(newlist,axis=1)
		newlist.columns = range(len(newlist.columns))
		newlist2.append(newlist)
		newlist = []
	newlist2 = pd.concat(newlist2)
	newlist2.columns = range(len(newlist2.columns))
	newlist2.index = range(len(newlist2.index))

	return newlist2

# given a list of precisions returns a dataframe of hashs between the two points
# creates a dummy set of geohashs between any two precisions
def create_interval_table(count1,count2):
	current = count1 
	if float(count1) / 2.0 == int(count1) / 2:
		initial = make_hash_df8()
	else:
		initial = make_hash_df7()

	currenttable = initial
	while not current == count2:
		current += 1
		currenttable = add_hashtable(currenttable,current)
	return currenttable


# given a base hash and a precision to go to creates a
# hash table that completes the table to the precision given
def make_hashtable(basehash,precision):
	firstcount = len(basehash) + 1

	basetable = create_interval_table(firstcount,precision)

	return add_prefix(basehash,basetable)


# given the first and last hash hash returns the shift an index will have to make
# against a geohash interval in which the corner is in the middle of a geohash level
def get_shift(firsthash,leveldict,level):
	starthashlevel = str(firsthash[-level])
	startindex = leveldict[starthashlevel]
	return [-startindex[0],-startindex[1]]

# gets the number of alike geohash precisions in the first and last geohash
def get_initial_size(firsthash,lasthash):
	count = 0
	for a,b in itertools.izip(firsthash,lasthash):
		if not str(a) == str(b):
			return count 
		count += 1

# makes a hashtable for a given range of geohashs and fills 
# geohashs to the given precision
def make_hashtable_range(firsthash,lasthash,**kwargs):
	global add_table
	precision = len(firsthash)
	for keyvalue,value in kwargs.iteritems():
		if key == 'precision':
			precision = value
	dicteven = {'1': [0, 6], '0': [0, 7], '3': [1, 6], '2': [1, 7], '5': [0, 4], '4': [0, 5], '7': [1, 4], '6': [1, 5], '9': [2, 6], '8': [2, 7], 'c': [3, 6], 'b': [3, 7], 'e': [2, 4], 'd': [2, 5], 'g': [3, 4], 'f': [3, 5], 'h': [0, 3], 'k': [1, 3], 'j': [0, 2], 'm': [1, 2], 'n': [0, 1], 'q': [1, 1], 'p': [0, 0], 's': [2, 3], 'r': [1, 0], 'u': [3, 3], 't': [2, 2], 'w': [2, 1], 'v': [3, 2], 'y': [3, 1], 'x': [2, 0], 'z': [3, 0]}
	dictodd = {'1': [1, 3], '0': [0, 3], '3': [1, 2], '2': [0, 2], '5': [3, 3], '4': [2, 3], '7': [3, 2], '6': [2, 2], '9': [1, 1], '8': [0, 1], 'c': [1, 0], 'b': [0, 0], 'e': [3, 1], 'd': [2, 1], 'g': [3, 0], 'f': [2, 0], 'h': [4, 3], 'k': [4, 2], 'j': [5, 3], 'm': [5, 2], 'n': [6, 3], 'q': [6, 2], 'p': [7, 3], 's': [4, 1], 'r': [7, 2], 'u': [4, 0], 't': [5, 1], 'w': [6, 1], 'v': [5, 0], 'y': [6, 0], 'x': [7, 1], 'z': [7, 0]}

	# instanitating hash7 and hash8
	hash7 = make_hash_df7()
	hash8 = make_hash_df8()

	intial = get_initial_size(firsthash,lasthash)
	current = intial
	count = 0
	while not current == precision:
		current += 1
		if float(current) / 2.0 == current / 2:
			dictcurrent = dicteven
			shape = [8,4]
			base = hash8
		else:
			dictcurrent = dictodd
			shape = [4,8]
			base = hash7

		if current <= len(lasthash):
			letterfirst,letterlast = firsthash[current-1],lasthash[current-1]

			# reindexing the add_table global
			index1,index2 = dictcurrent[str(letterfirst)],dictcurrent[str(letterlast)]
			x1,x2 = index1[0],index2[0]
			y1,y2 = index1[1],index2[1]
			xmin,xmax = min([x1,x2]),max([x1,x2])
			ymin,ymax = min([y1,y2]),max([y1,y2]) 
			add_table = base.loc[range(ymin,ymax+1)][range(xmin,xmax+1)]

		# setting intial base
		if count == 0:
			currentbase = add_table
			currentbase = add_prefix(lasthash[:intial],currentbase)
		elif current > len(lasthash):
			add_table = base
			currentbase = add_hashtable(currentbase,current)
		else:
			# updating the current base with a new set of geohash levels
			currentbase = add_hashtable(currentbase,current)

			# getting indexs so that correct positions are selected respectively
			index1 = get_index(currentbase,firsthash[:current])
			index2 = get_index(currentbase,lasthash[:current])
			x1,x2 = index1[0],index2[0]
			y1,y2 = index1[1],index2[1]
			xmin,xmax = min([x1,x2]),max([x1,x2])
			ymin,ymax = min([y1,y2]),max([y1,y2]) 
			currentbase = currentbase.loc[range(ymin,ymax+1)][range(xmin,xmax+1)]
		count += 1

	currentbase.columns = range(len(currentbase.columns))
	currentbase.index = range(len(currentbase.index))

	return currentbase

# given a hashtable and geohash within the table finds the index 
# in a heirachical method
def get_index(hashtable,hash):
	dicteven = {'1': [0, 6], '0': [0, 7], '3': [1, 6], '2': [1, 7], '5': [0, 4], '4': [0, 5], '7': [1, 4], '6': [1, 5], '9': [2, 6], '8': [2, 7], 'c': [3, 6], 'b': [3, 7], 'e': [2, 4], 'd': [2, 5], 'g': [3, 4], 'f': [3, 5], 'h': [0, 3], 'k': [1, 3], 'j': [0, 2], 'm': [1, 2], 'n': [0, 1], 'q': [1, 1], 'p': [0, 0], 's': [2, 3], 'r': [1, 0], 'u': [3, 3], 't': [2, 2], 'w': [2, 1], 'v': [3, 2], 'y': [3, 1], 'x': [2, 0], 'z': [3, 0]}
	dictodd = {'1': [1, 3], '0': [0, 3], '3': [1, 2], '2': [0, 2], '5': [3, 3], '4': [2, 3], '7': [3, 2], '6': [2, 2], '9': [1, 1], '8': [0, 1], 'c': [1, 0], 'b': [0, 0], 'e': [3, 1], 'd': [2, 1], 'g': [3, 0], 'f': [2, 0], 'h': [4, 3], 'k': [4, 2], 'j': [5, 3], 'm': [5, 2], 'n': [6, 3], 'q': [6, 2], 'p': [7, 3], 's': [4, 1], 'r': [7, 2], 'u': [4, 0], 't': [5, 1], 'w': [6, 1], 'v': [5, 0], 'y': [6, 0], 'x': [7, 1], 'z': [7, 0]}

	# getting last position hash
	tableshape = hashtable.shape
	lasthash = hashtable[tableshape[1]-1][tableshape[0]-1]
	firsthash = hashtable[0][0]

	# getting the number of characters alike in the string from the beg.
	intialsize = get_initial_size(firsthash,lasthash)

	# start point 
	start = len(hash) - intialsize
	count = 0
	totalindex = [0,0]
	precision = intialsize
	while not start == 0:
		precision += 1
		if float(precision) / 2.0 == precision / 2:
			dictcurrent = dicteven
			shape = [8,4]
		else:
			dictcurrent = dictodd
			shape = [4,8]
		hashletter = hash[-start]
		start -= 1
		currentindex = dictcurrent[str(hashletter)]

		# logic or function for shift here
		if precision <= len(firsthash):
			shift = get_shift(firsthash,dictcurrent,start+1)
		else:
			shift = [0,0]
		
		# shifting the current index against the found position from get_shift()
		currentindex = [currentindex[0] + shift[0], currentindex[1] + shift[1]]

		# multiplying each levvel by the dimmension of each
		if not start == 0:
			totalindex = [(totalindex[0] * shape[0]) + (currentindex[0] * shape[0]),(totalindex[1] * shape[1]) + (currentindex[1] * shape[1]) ]
		else:
			totalindex = [totalindex[0] + currentindex[0], totalindex[1] + currentindex[1]]
		

	return totalindex

# given 2 hashs returns a hashtable df returns a dataframe inbetween the two hashs
def multi_index(hashtable,hash1,hash2):
	index1 = get_index(hashtable,hash1)
	index2 = get_index(hashtable,hash2)

	# getting indicies ranges for indexing against the hashtable
	x1,x2 = index1[0],index2[0]
	y1,y2 = index1[1],index2[1]
	xmin,xmax = min([x1,x2]),max([x1,x2])
	ymin,ymax = min([y1,y2]),max([y1,y2]) 


	return hashtable.loc[range(ymin,ymax+1)][range(xmin,xmax+1)]

# given a single hash within a hash table one-d lower then then the hashtable contents
# creates the hashtable representing the range of hash given and selects the ul corner and lr corner
def multi_index_single(hashtable,hashval):
	# getting the two hashs representing the ranges within truncated hashval
	precision = len(hashval) + 1
	rangehashtable = add_hashtable(pd.DataFrame([hashval]),precision)
	rangeshape = rangehashtable.shape
	hash1 = rangehashtable[0][0]
	hash2 = rangehashtable[rangeshape[1]-1][rangeshape[0]-1]

	# getting the indicies from each hash
	index1 = get_index(hashtable,hash1)
	index2 = get_index(hashtable,hash2)

	# getting indicies ranges for indexing against the hashtable
	x1,x2 = index1[0],index2[0]
	y1,y2 = index1[1],index2[1]
	xmin,xmax = min([x1,x2]),max([x1,x2])
	ymin,ymax = min([y1,y2]),max([y1,y2]) 


	return hashtable.loc[range(ymin,ymax+1)][range(xmin,xmax+1)]


# makes the master list of indicies that will be used to build the entire table
def make_indicies(x,y):
	xs = range(x)
	ys = range(y)
	newlist = []
	for row in xs:
		#print row
		#print ys
		row = zip([row]*len(ys),ys)
		#row = list(zip([row]*len(ys),ys))
		row = pd.DataFrame(row,columns=['X','Y'])
		row['string'] = row['X'].astype(str) + ',' + row['Y'].astype(str)
		row = row['string'].values.tolist()
		newlist.append(row)

	newlist = pd.DataFrame(newlist)
	newlist = newlist.T
	return newlist


# returns index strings of hashtable of the same shape as input
def make_indexs(hashtable): 
	xshape,yshape = hashtable.shape[1],hashtable.shape[0]
	return make_indicies(xshape,yshape)


def gener(list):
	for row in list:
		yield row


def get_sides(lineindex,xshape,yshape):
	side2 = False
	side1 = False
	if lineindex[0][0] == 0:
		side1 = 'west'
	elif lineindex[0][1] == 0:
		side2 == 'north'
	if lineindex[0][0] == xshape:
		side1 = 'east'
	elif lineindex[0][1] == yshape:
		side2 = 'south'
	if lineindex[-1][0] == xshape:
		side1 = 'east'
	elif lineindex[-1][1] == yshape:
		side2 = 'south'
	if lineindex[-1][0] == 0:
		side1 = 'west'
	elif lineindex[-1][1] == 0:
		side2 = 'north'

	if side2 == False:
		side2 = side1
	if side1 == False:
		side1 = side2

	return [side1,side2]


# logic for doing the orientation of the sides and returning bools which dictate 
# the way in which data is selected
def orienation_logic(sides,orientation):
	if sides[0] == sides[1]:
		if orientation == 'west':
			return [False,False]
		if orientation == 'east':
			return [False,True]
		if orientation == 'north':
			return [True,False]
		if orientation == 'south':
			return [False,False]
	if sides[1] == 'south' and orientation == 'north':
		northbool = True
		if sides[0] == 'east':
			westbool = False
		else:
			westbool =True
	elif sides[1] == 'south' and orientation == 'east':
		northbool = True
		if sides[0] == 'east':
			westbool = False
		else:
			westbool = True
	elif sides[1] == 'north' and orientation == 'east':
		northbool = False
		if sides[0] == 'east':
			westbool = True
		else:
			westbool = False
	elif sides[1] == 'north' and orientation == 'south':
		northbool = False
		if sides[0] == 'east':
			westbool = True
		else:
			westbool = False
	if sides[1] == 'south' and orientation == 'south':
		northbool = False
		if sides[0] == 'east':
			westbool = True
		else:
			westbool = False



	return [northbool,westbool]


def get_y_points(lineindex,y):
	newlist = []
	for row in lineindex:
		if row[1] == y:
			newlist.append(row)
	return newlist


def get_points(oldrow,indexlist):
	xs = []
	for row in oldrow.values.tolist():
		oldval = row
		for row in indexlist:
			if str(row[0]) + ',' + str(row[1]) == oldval:
				xs.append(row)	
	return xs


# returns the geohashs within an area for a same side entrance and exit
def get_area_complete(indexdf,lineindex,orientation,xshape,yshape):
	if lineindex[0][0] == 0 and lineindex[-1][0] == 0:
		side = 'west'
	if lineindex[0][1] == 0 and lineindex[-1][1] == 0:
		side = 'north'
	if lineindex[0][0] == xshape and lineindex[-1][0] == xshape:
		side = 'east'
	if lineindex[0][1] == yshape and lineindex[-1][1] == yshape:
		side = 'south'
	newlist = []

	if side == 'west':
		rowind = 0
		count = 0
		rowpos = False
		rowpos1 = False
		rowpos2 = False
		rowbool = False
		for row in indexdf.values.tolist():
			oldrow = row
			y = int(oldrow[0][-1])
			points = get_y_points(lineindex,y)
			for row in oldrow:
				for point in points:
					if row == str(point[0]) + ',' + str(point[1]):
						rowpos = count
						rowbool = True
					if len(points) > 1 and not rowind == 0:
						rowpos1 = rowpos
						rowind = 1
					elif len(points) > 1:
						rowpos2 = rowpos
				count += 1
			if rowind == 0 and rowbool == True:
				if 'inner' == orientation:
					newlist += oldrow[:rowpos+1]
				elif 'outer' == orientation:
					newlist += oldrow[rowpos:]
			count = 0
			rowind =0
			rowpos = False
			rowpos1 = False
			rowpos2 = False
			rowbool = False
	if side == 'east':
		rowind = 0
		count = 0
		rowpos = False
		rowpos1 = False
		rowpos2 = False
		rowbool = False
		for row in indexdf.values.tolist():
			oldrow = row
			y = int(oldrow[0][-1])
			points = get_y_points(lineindex,y)
			for row in oldrow:
				for point in points:
					if row == str(point[0]) + ',' + str(point[1]):
						rowpos = count
						rowbool = True
					if len(points) > 1 and not rowind == 0:
						rowpos1 = rowpos
						rowind = 1
					elif len(points) > 1:
						rowpos2 = rowpos
				count += 1
			if rowind == 0 and rowbool == True:
				if 'inner' == orientation:
					newlist += oldrow[:rowpos+1]
				elif 'outer' == orientation:
					newlist += oldrow[rowpos:]
			count = 0
			rowind =0
			rowpos = False
			rowpos1 = False
			rowpos2 = False
			rowbool = False
		return newlist
	if side == 'north':
		count = 0
		rowpos = False
		rowpos1 = False
		rowpos2 = False
		rowbool = False
		rowindex = 0
		rowind = 0
		rowindexs = []
		for row in indexdf.columns.values.tolist():
			oldrow = indexdf[row]
			xs = get_points(oldrow,lineindex)
			for row in xs:
				oldpoint = row
				for row in indexdf[oldpoint[0]].values.tolist():
					if int(row[-1]) == oldpoint[1]:
						indexposition = int(row[-1])
						if orientation == 'inner':
							newlist += indexdf[oldpoint[0]][:indexposition+1].values.tolist()
						elif orientation == 'outer':
							newlist += indexdf[oldpoint[0]][indexposition:].values.tolist()
		return newlist
	if side == 'south':
		count = 0
		rowpos = False
		rowpos1 = False
		rowpos2 = False
		rowbool = False
		rowindex = 0
		rowind = 0
		rowindexs = []
		for row in indexdf.columns.values.tolist():
			oldrow = indexdf[row]
			xs = get_points(oldrow,lineindex)
			for row in xs:
				oldpoint = row
				for row in indexdf[oldpoint[0]].values.tolist():
					if int(row[-1]) == oldpoint[1]:
						indexposition = int(row[-1])
						if orientation == 'inner':
							newlist += indexdf[oldpoint[0]][:indexposition+1].values.tolist()
						elif orientation == 'outer':
							newlist += indexdf[oldpoint[0]][indexposition:].values.tolist()
		return newlist


# lineindex can be a complete area in which orientation can be
# 	inner
# 	outer 
# for the area you give (generally used when one side contains both entering and leaving)
# o x x x
# o o x x
# o x x x
# area below would be [[0,0],[1,1],[0,2]] inner
# orientation regarding an edge i.e. one side to one side can be defined by 
# any directionality ('north','south','east','west') that should define all
def make_area(indexdf,lineindex,orientation):
	# getting line index generator 
	generarea = gener(lineindex)
	linepoint = next(generarea)

	# establishing corner indexs
	tableshape = indexdf.shape
	xshape = tableshape[1] - 1
	yshape = tableshape[0] - 1
	nw = [0,0]
	ne = [xshape,0]
	sw = [0,yshape]
	se = [xshape,yshape]

	if orientation == 'inner' or orientation == 'outer':
		return get_area_complete(indexdf,lineindex,orientation,xshape,yshape)

	# getting sides of the slice
	if orientation == 'north' or orientation == 'south' or orientation == 'west' or orientation == 'east':
		sides = get_sides(lineindex,xshape,yshape)


	# getting the indicator bools for the directionaility of the area
	northbool,westbool = orienation_logic(sides,orientation)

	northind = 0
	count = 0
	rowpos = False
	newlist = []
	firstind = 0
	count2 = 0
	indexpos = False
	# iterating through each row then each value
	for row in indexdf.values.tolist():
		oldrow = row
		count2 += 1
		for row in oldrow:
			if row == str(linepoint[0]) + ',' + str(linepoint[1]):
				if firstind == 0:
					rowpos = count
				northind = 1
				try:
					linepoint = next(generarea)
				except StopIteration:
					indexpos = count2 
				firstind = 1
			count += 1
		# logic for partial rows
		if firstind == 1 and westbool == True:
			newlist += oldrow[rowpos:]
		if firstind == 1 and westbool == False:
			newlist += oldrow[:rowpos+1]
		rowpos = False
		count = 0
		firstind = 0

		# logic for full rows
		if northbool == True and northind == 0 and not indexpos == False:
			newlist += oldrow
	
	# logic for slices of the entire df selecting entire columns
	if northbool == True and len(newlist) == 1:
		newlist = indexdf.values.tolist()[:indexpos]
		newlist2 = []
		for row in newlist:
			newlist2 += row
		newlist = newlist2
	elif northbool == False and len(newlist) == 1:
		newlist = indexdf.values.tolist()[indexpos:]
		newlist2 = []
		for row in newlist:
			newlist2 += row
		newlist = newlist2

	return newlist


# given a index value returns bool for mapping
def make_bool(indexval):
	global indexlist
	for row in indexlist:
		if row == indexval:
			return True
	return False

# makes dataframe bool for whether or not the geohash is added
def make_df_bool(indexdf,indexes):
	global indexlist
	indexlist = indexes
	return indexdf.applymap(make_bool)

# gets an area within an indexstring of a mapped dataframe
def get_area(indexstring):
	global bools
	global geohashs
	indexes = str.split(indexstring,',')
	x,y = int(indexes[0]),int(indexes[1])

	if bools[x][y] == True:
		return geohashs[x][y]
	else:
		return ''


# returns list of valid geohashs that encompass area
def make_area_geohashs(indexdf,booldf,hashtable):
	global bools
	global geohashs
	bools = booldf
	geohashs = hashtable

	values = indexdf.applymap(get_area)
	#values =  values.unstack(level=0).reset_index()
	return values


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




# for a given set of indices makes all indicies between given sequential indexs
def fill_indicies(indexlist,indexdf):
	count = 0
	oldrow2 = 0
	newlist2 = []
	for row in indexlist:
		if count == 0:
			count = 1
		else:
			data = pd.DataFrame(generate_points(len(indexdf.unstack(level=0))*2,oldrow,row)[1:],columns=['X','Y'])
			data = data.round(decimals=0)
			newlist = []
			count2 = 0
			for row in data.values.tolist():
				if count2 == 0 and not oldrow2 == row:
					count2 = 1
					newlist.append([int(row[0]),int(row[1])])
					oldrow2 = row
				else:
					if not oldrow2 == row:
						newlist.append([int(row[0]),int(row[1])])
						oldrow2 = row
			newlist2 += newlist
		oldrow = row
	return newlist2


# given a list of indicies being several indicies within a given hashtable
# creates a stepped index of all indicies in between stair stepping the float 
# values that have indicies in between
# syntax require entry and exit to be clearly marked for segments (i.e. a index for in and out)
def linear_cut(indexdf,hashtable,indexlist,orientation,**kwargs):
	skip_fill = False
	for key,value in kwargs.iteritems():
		if key == 'skip_fill':
			skip_fill = value
	
	if skip_fill == False:
		# filling in index list for all possible indexs
		indexlist = fill_indicies(indexlist,indexdf)

	# making area using the orientation given
	# logic may be added for index list to automatically check against 
	# complete areas
	indexlist = make_area(indexdf,indexlist,orientation)
	booldf = make_df_bool(indexdf,indexlist)
	hashtable = pd.DataFrame(make_area_geohashs(indexdf,booldf,hashtable))#.unstack(level=0).reset_index()
	return hashtable#[0].values.tolist()

# given a list of indicies being several indicies within a given hashtable
# creates a stepped index of all indicies in between stair stepping the float 
# values that have indicies in between
# syntax require entry and exit to be clearly marked for segments (i.e. a index for in and out)
def linear_cut(indexdf,hashtable,indexlist,orientation):
	# filling in index list for all possible indexs
	indexlist = fill_indicies(indexlist,indexdf)

	# making area using the orientation given
	# logic may be added for index list to automatically check against 
	# complete areas
	indexlist = make_area(indexdf,indexlist,orientation)
	booldf = make_df_bool(indexdf,indexlist)
	hashtable = pd.DataFrame(make_area_geohashs(indexdf,booldf,hashtable))#.unstack(level=0).reset_index()
	return hashtable#[0].values.tolist()

# make
def edge(index):
	global indicies
	x,y = str.split(index,',')
	x,y = int(x),int(y)
	for row in indicies:
		if x == row[0] and y == row[1]:
			return index
	return False

# makes geohashs within a set of indicies given a hashtable
def make_hashs(indicies,hashtable):
	newlist = []
	for row in indicies:
		x,y = str.split(row,',')
		x,y = int(x),int(y)	
		newlist.append(hashtable[x][y])
	return newlist

# returns a bool dataframe containing the the two segemented areas
def shade(indexdf,hashtable,indexlist):
	# filling in index list for all possible indexs
	indexlist = fill_indicies(indexlist,indexdf)
	global indicies
	indicies = indexlist
	indexmap = indexdf.applymap(edge)
	for row in indexmap.columns.values.tolist():
		oldrow = row
		count = 0
		newlist = []
		ind = False
		for row in indexmap[oldrow]:
			if not row == False or ind == True:
				newlist.append(True)
				oldval = count
			elif row == False:
				newlist.append(False)
				oldval = False
			count += 1
		if newlist[0]==True and newlist[-1] == True:
			ind = True
		indexmap[oldrow] = newlist

	return indexmap

# maps geohashs to a dataframe giving a  bool df
def gethashs(indstring):
	global hashtableind
	x,y = str.split(indstring,',')
	x,y = int(x),int(y)
	return hashtableind[x][y]

def make_shade(booltable,hashtable,bool):
	global hashtableind
	hashtableind = hashtable
	# unstacking and setting up for mapping operation
	booltable = booltable.unstack(level=0).reset_index()
	booltable.columns = ['X','Y','BOOL']
	booltable = booltable[booltable['BOOL'] == bool]
	booltable['INDSTR'] = booltable['X'].astype(str) + ',' + booltable['Y'].astype(str)
 	
	# mapping geohashs about index field
 	booltable['GEOHASH'] = booltable['INDSTR'].map(gethashs)
 	
 	return booltable['GEOHASH'].tolist()

# returns simply an dataframe represention of ddf
def make_shade2(booltable,hashtable,bool):
	global hashtableind
	hashtableind = hashtable
	# unstacking and setting up for mapping operation
	booltable = booltable.unstack(level=0).reset_index()
	booltable.columns = ['X','Y','BOOL']
	booltable = booltable[booltable['BOOL'] == bool]
	booltable['INDSTR'] = booltable['X'].astype(str) + ',' + booltable['Y'].astype(str)
 	
	# mapping geohashs about index field
 	booltable['GEOHASH'] = booltable['INDSTR'].map(gethashs)
	return booltable

# given a set of geohashs returns a centroid of the collection of geometries
def get_centroid(geohashs,entirehashtable):
	newlist = [['GEOHASH','x','y']]
	for row in geohashs:
		x,y = get_index(entirehashtable,row)
		newlist.append([row,x,y])
	newlist = pd.DataFrame(newlist[1:],columns=newlist[0])
	return [newlist['x'].mean(),newlist['y'].mean()]

# given a point1 x,y and a point2 x,y returns distance in miles
# points are given in long,lat geospatial cordinates
def distance(point1,point2):
	x1,x2 = point1[0],point2[0]
	y1,y2 = point1[1],point2[1]

	cord_distance = (((x2 - x1) ** 2) + ((y2 - y1) ** 2)) ** .5 

	return cord_distance


 # given a comparison centroid, an entirehashtable,thehashtable for 
 # booldf returns the set of geohashs which correspond whichever
 # set of areas global centroid is closer to the global centroid about the entire area
def get_centroid_geohashs(centroid,entirehashtable,hashtable,booldf):
 	# getting the true area collection and the false area collection
 	truevalues = make_shade(booldf,hashtable,True)
 	falsevalues = make_shade(booldf,hashtable,False)

 	centroid1 = get_centroid(truevalues,entirehashtable)
 	centroid2 = get_centroid(falsevalues,entirehashtable)

 	dist1 = distance(centroid1,centroid)
 	dist2 = distance(centroid2,centroid)

 	if dist1 > dist2:
 		return falsevalues
 	else:
 		return truevalues

# given the first and last hash hash returns the shift an index will have to make
# against a geohash interval in which the corner is in the middle of a geohash level
def get_shift(firsthash,leveldict,level):
	starthashlevel = str(firsthash[-level])
	startindex = leveldict[starthashlevel]
	return [-startindex[0],-startindex[1]]
def get_xy(inds,columns,index,xmax,ymax):
	newx, newy = inds[0],inds[1]
	if newx < 0 and not abs(newx) > xmax:
		x = columns[-newx]
	elif abs(newx) > xmax and newx < 0:
		x = columns[newx + xmax]
	else:
		x = columns[newx]

	if newy < 0 and not abs(newy) > ymax:
		y = index[-newy]
	elif abs(newy) > ymax and newy < 0:
		y = index[newy + ymax]
	else:
		y = index[newy]
	return [x,y]

def get_corner(hash,corner):	
	lat,long,latdelta,longdelta = geohash.decode_exactly(hash)

	# ul corner
	if corner == 'ul':
		lat = lat + (3 * latdelta)
		long = long - (3 * longdelta)
		return geohash.encode(lat,long,len(hash))
	elif corner == 'lr':
		lat = lat - (3 * latdelta)
		long = long + (3 * longdelta)
		return geohash.encode(lat,long,len(hash))
