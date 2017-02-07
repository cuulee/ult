import pandas as pd
import time
import itertools
import simplejson as json
from .polygon_dict import make_json_string,reduce_to_min,area_index
import numpy as np
import future

def get_df(index):
	return pd.DataFrame(index['ultindex'].items(),columns=['GEOHASH','AREA'])

# makes a json string representation given a df or dictionary object
def make_json_string_combined(data,intbool=False):
	if isinstance(data,dict):
		data = pd.DataFrame(data.items(),columns=['GEOHASH','AREA'])

	data1 = data[data['AREA'] == -1]
	data2 = data[data['AREA'] != -1]

	# creating output string for dataframe
	data1['STRING'] = '"' + data['GEOHASH'].astype(str) + '":' + data['AREA'].astype(str) 	
	data2['STRING'] = '"' + data['GEOHASH'].astype(str) + '":"' + data['AREA'].astype(str) + '"'
	data = pd.concat([data1,data2])
	
	data['a'] = 'z'
	return '{%s}' % data.groupby('a')['STRING'].apply(lambda x:"%s" % ', '.join(x))['z']

def drill_lower(basedf,header):
	basedf = basedf.fillna(value=-10000)
	newlist = []
	for i in header:
		temp = basedf[basedf[i] == -10000]
		newlist.append(temp)
	here = pd.concat(newlist)
	geohashdict = here.set_index('GEOHASH')['AREA'].to_dict()
	here['EXPAND'] = here['GEOHASH'].map(map_expand)
	here['a'] = 'a'
	string = here.groupby('a')['EXPAND'].apply(lambda x:"%s" % ','.join(x)).reset_index()['EXPAND'].loc[0]
	here = pd.DataFrame(str.split(string,','),columns=['GEOHASH'])
	here['AREA'] = here['GEOHASH'].str[:-1].map(lambda x:geohashdict[x])
	return here

def map_expand(x):
	return '%s0,%s1,%s2,%s3,%s4,%s5,%s6,%s7,%s8,%s9,%sb,%sc,%sd,%se,%sf,%sg,%sh,%sj,%sk,%sm,%sn,%sp,%sq,%sr,%ss,%st,%su,%sv,%sw,%sx,%sy,%sz' % (x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x)


# combines an input of ultindex h5 files
# given in the proper hiearchy from base to top level
def combine_indexs(fileslist,fieldlist,filename):
	from polygon_dict import readh
	fieldlistint = fieldlist
	# getting all indexs
	indexs = [readh(i) for i in fileslist]
	mins = []
	maxs = []
	for i in indexs:
		minsize,maxsize = i['metadata']['minsize'],i['metadata']['maxsize']
		maxs.append(maxsize)
		mins.append(minsize)
	minsize = min(mins)
	maxsize = max(maxs)


	# getting base index that will be taken to a dataframe
	baseindex = indexs[0]
	indexs = indexs[1:]
	basefield = fieldlist[0]
	fieldlist = fieldlist[1:]


	# getting teh base dataframe
	basedf = get_df(baseindex)
	basedf[str(basefield)] = basedf['AREA']
	print('[1/6] Created base dataframe')
	
	# slicing data to remove upper hierarchies
	upperdf = basedf[basedf['AREA'] == -1]
	basedf = basedf[basedf['AREA'] != -1]

	# iterating through each remaining index and adding the appopriate field
	for index,header in zip(indexs,fieldlist):
		basedf = area_index(basedf,index,column=header,dummyoutput=True)
	print('[2/6] Completed initial index')
	
	# doing one layer of correction 
	here = drill_lower(basedf,fieldlist)
	count = 0
	ind = 0
	''''
	while ind == 0:
		if count == 0:
			here = drill_lower(basedf,fieldlist)
		else:
			here = drill_lower(here,fieldlist)

		for index,header in zip(indexs,fieldlist):
			here = area_index(here,index,column=header,dummyoutput=True)
		d = here.fillna(value=-10000)
		a = d[(d['DIST_AREA']==-10000)|(d['STATES_AREA']==-10000)]
		print a,count
		if len(a) == 0:
			ind = 1
		count += 1
	'''

	for index,header in zip(indexs,fieldlist):
		here = area_index(here,index,column=header,dummyoutput=True)
	#d = here.fillna(value=-10000)
	#a = d[(d['DIST_AREA']==-10000)|(d['STATES_AREA']==-10000)]
	here = here.fillna(value=-10000)
	print ('[3/6] Completed drill correction')

	# slicing the tmepf frame for each field
	newlist = []
	for i in fieldlist:
		temp = here[here[i] != -10000]
		newlist.append(temp)
	newvalues = pd.concat(newlist)
	newvalues[str(basefield)] = newvalues['AREA']
	print ('[4/6] Selected new values.')

	# creating bool to slice by
	basedf['BOOL'] = basedf['GEOHASH'].isin(
		np.unique(newvalues['GEOHASH'].str[:-1]))
	
	# slicing by whether or not the values were taking out arein the df
	basedf1 = basedf[basedf['BOOL'] == False][basedf.columns[:-1]]
	basedf2 = basedf[basedf['BOOL'] == True][basedf.columns[:-1]]
	
	# combining the created parts to make upperdf and based df
	upperdf = pd.concat([upperdf,basedf2[['GEOHASH','AREA']]])
	basedf = pd.concat([basedf1,newvalues])
	print('[5/6] Created basedf and upperdf')

	for i in fieldlistint:
		if not i == fieldlistint[-1]:
			basedf[i] = basedf[i].astype(str) + ','
		else:
			basedf[i] = basedf[i].astype(str)
	
	basedf['TEXT'] = basedf[fieldlistint[0]]
	
	for i in fieldlistint[1:]:
		basedf['TEXT'] = basedf['TEXT'] + basedf[i]
	print('[6/6] Creating text field')

	# finally creating the dictionary object
	basedf = basedf.set_index('GEOHASH')['TEXT'].to_dict()
	for i in upperdf['GEOHASH'].values.tolist():
		basedf[i] = -1

	# setting the base df as the new ultindex
	metadata = baseindex['metadata']
	metadata['minsize'] = minsize
	metadata['maxsize'] = maxsize
	metadata['headers'] = fieldlistint
	metadict = json.dumps(metadata)

	# area mask stuff
	areamask = json.dumps(baseindex['areamask'])

	# making json string
	jsonstring = make_json_string_combined(basedf)

	# creating dataframe wrappper
	data = pd.DataFrame([jsonstring,metadict,areamask],index=['ultindex','metadata','areamask'])

	# writing to output file
	with pd.HDFStore(filename) as out:
		out['combined'] = data
		out['alignmentdf'] = baseindex['alignmentdf']

# makes a test block and returns the points for the index
def make_test_block(ultindex,number,headerval=False):
	from mapkit import make_colorkey
	from pipegeohash import map_table,random_points_extrema
	if headerval == False:
		headerval = ultindex['metadata'].get('headers','')
		if not headerval == '':
			headerval = headerval[0]
	extrema = {'n': 50.449779,'s': 20.565774,'e':-64.493017,'w':-130.578836}
	data = random_points_extrema(number,extrema)
	data = map_table(data,12,map_only=True)
	s = time.time()
	data = area_index(data,ultindex)
	if not headerval == '':
		data = make_colorkey(data,headerval,hashfield=True,small=True)
	else:
		data = data[data['AREA'].astype(str).str.len() > 0]
		data = make_colorkey(data,'AREA',hashfield=True,small=True)
	data['color'] = data['COLORKEY']
	return data

#print ult.readh('counties.h5')['metadata']
#print ult.readh('dists.h5')['metadata']
#combine_indexs(['counties.h5','dists.h5','states.h5'],['COUNTIES_AREA','DIST_AREA','STATES_AREA'],'a.h5')
#combine_indexs(['dists.h5','states.h5'],['DIST_AREA','STATES_AREA'],'a.h5')

#index = ult.readh('a.h5')
#index['metadata']['minsize'] = 3
#print index['ultindex'].items()[:10]
#data = make_test_block(index,1000000,headerval='STATES_AREA')
#d = pd.read_csv('d.csv')
#mk.make_map([mk.make_colorkey(d,'GEOHASH'),'blocks'])
#mk.make_map([[data,'points']])

