# Spatial Aggregations Homie

Polygon Index / Linstring Index for spatial aggregations

Geospatial problems are hard, especially when having to relatee a set of geometries (lines or polygons) to a substantial set of points, traditional methodologies essentially are infeasible over 100k + points without huge computing time lost. The solution is pretty simple remove geometry entirely from the operation. Like many things in CS this trades precomputation time of the indexs with the performence boost of using said indicies. 


##### Pictures
![](https://cloud.githubusercontent.com/assets/10904982/19921148/0080300e-a0b3-11e6-9395-add3308f2238.png)
![](https://cloud.githubusercontent.com/assets/10904982/19921150/0395f56c-a0b3-11e6-93fe-00166d12e08f.png)
![](https://cloud.githubusercontent.com/assets/10904982/19921175/19c1e990-a0b3-11e6-8284-3563b183ac17.png)


The nice thing about whats these indexs are made is, extracting the features from them on say a dataframe of points is incredibly easy:
* geohash the entire table 
* map the geohash column on the index until error or value is returned. 

The index also contains every single area given instead of what would be a point comparison against 50 diffent geometries in a typical geometric comparison. 
Benchmarks in comparison against shapely:
```
SHAPELY | 0.027808
ULT | 0.001157
dtype: float64
24.0287388881x 100 points
SHAPELY    2.367857
ULT        0.009810
dtype: float64
241.381324357x 10000 points
SHAPELY    24.440322
ULT         0.095100
dtype: float64
256.996006301x 100000 points
```
**Its also worth noting that this is a one polygon comparison ult can have as many polygons within its index as it needs and see little hit in performence, while I'm not sure how you would do this with out a hard for loop through every geometry in shapely**

### How it works / How to Use
#### Polygons 
For polygons ult is pretty strict on how it accepts tables to be made into indexs entirely you can look at a csv I added as example, but generally speaking I usually use another repository I wrote called KML which interpets all the polygons for me and outputs into a table that be directly used by ult. When ult creates an index from a table its essentially compiling a multi dimmensional dictionary object that will be output as a json. To use this json simply read it into memory and send into the function area_index. 

Building an index isn't necessarily cheap or quick computationally speaking. Building an index for the 50 states and other provinces took about 611 seconds. So I've tried to add functionality that allows you to resume building an index if for some reason it quits mid process to do this use the kwargs, resume=True, the folder kwarg to output your csv file for each individual area, and csv = True kwarg. This will write a csv file to a specific folder in your directory of the index that was made so that it can be read into memory entirely at the end to make your index. Essentially these are just features put into place to save time.

For huge indexs you may want to utilize multi processesing in order to create the csv files used to create an index faster, I've added this functionality as well using the sc=sc kwarg in which you pass in a spark context and splits the remaining areas up into 8 supprocesses. 

While the indexs created aren't tiny by any means there also not huge, I've attempted to be as conservative as possible in making the indexs as small as possible and it works pretty well, a [states_index.json](https://raw.githubusercontent.com/murphy214/ult/master/states_index.json) is about 1.2 mb which isn't terrible and probably smaller than the amount of memory for every alignment in a list. 


**NOTE: Some of what I've stated above and the examples below may be deprecated I've changed a lot of the outputs to write a more dynamic h5 file and updated a ton of other stuff.**

Below is a small sample of the code to build and code to use these indexs:
#### Building 
```python
import pandas as pd
import ult

# reading table into memory
polygon_table = pd.read_csv('total_states.csv')


ult.make_set(polygon_table,'AREA','states_index.json')
```

#### Using 
```python
import pandas as pd
import ult
import time
import berrl as bl
#import nlgeojson as nl
#import pipevts as vt


states_index = ult.read_json('states_index.json')

# creating test data 
extrema = {'n':88.9,'s':-30.0,'e':100.0,'w':-180.0}
data = bl.random_points_extrema(1000000,extrema)

# geohashing each point
data = bl.map_table(data,12,map_only=True)

# now mapping each point to an area index
s = time.time()
data = ult.area_index(data,states_index)
totaltime = time.time() - s
timeperpoint = totaltime / float(len(data))


print data
print 'totaltime per %s points: %s' % (len(data),totaltime)
print 'time-per-point %s ' % timeperpoint

'''
# visualization shit 
data = data[data['AREA'].str.len() > 0]
data = bl.unique_groupby(data,'AREA')
nl.make_points(data,'points.geojson',mask=True)
vt.a()
```
