# Polygon Index / Linestring Index for fast Spatial aggregations

Geospatial problems are hard, especially when having to relatee a set of geometries (lines or polygons) to a substantial set of points, traditional methodologies essentially are infeasible over 100k + points without huge computing time lost. The solution is pretty simple remove geometry entirely from the operation. Like many things in CS this trades precomputation time of the indexs with the performence boost of using said indicies. 


##### Pictures
![](https://cloud.githubusercontent.com/assets/10904982/20707851/3e5f16e4-b5fb-11e6-9d9e-40c0fd3d6fb8.png)
![](https://cloud.githubusercontent.com/assets/10904982/20707852/3e6581f0-b5fb-11e6-9ba0-a19e68ba2b86.png)

# What is ult?
<p align="center">
  <img src="https://cloud.githubusercontent.com/assets/10904982/20707857/406493e2-b5fb-11e6-94de-afa550c2f09c.gif" alt="Polygon Aggregation"/>
</p>

Ult solves traditional geospatial problems in a way that relies heavily on the fastest localization method that I'm aware of for 2-d objects: geohashing. 

Geohashing is way of taking a 2-d object (lat/longs) and turning it into a 1-d string that importantly displays/conveys hiearchy. Ult takes the following geohash repo in python written in C++ that can geohash approximately 400k points / second and geospatially relates a point to a specific object(s) being either lines or polygons. 

While the geohashing process may be slowish this type of localization allows for multiple indexs that use geohashing to work seamlessly, and as fast as possible. 

In other words, assuming no memory constraints, any ultindex applied to a set of points should run about the same speed no matter how many polygons or lines is within the index. For example you could have an ultindex for the US states (50 polygon), and another one for zipcodes (35k polygons) and applying each of these indexs to a set of 1 millions points would return results in 1 second for each ultindex. You could also apply an index for road or other lines at basically the same speed. 

# What is an ultindex object?
An ultindex is a deepdish h5 file that contains the following data structures:
* alignmentdf - a pandas dataframe that was the input to make the ultindex this object can easily be output to geojson
* ultindex - a flat dictionary that utilizes the geohashs hierarchy that is assembled slightly different depending on whether the index represents lines or polygons 
* areamask - a dictionary mask that saves bytes or size on the large ultindex by having a key value structure for area or line ids to a hexidecimal range so a hexidecimal number can be used in the heavily repeated ultindex instead of a long id or area. 
* metadata - contains information about the type of ultindex that is being used 

This data structure can be directly sent into a localization algorithm and determine how the localization algorithm needs to be applied based on the metadata. It can also be used to easily output/style the data structure your aggregating against and output to a geojson file.


# Benchmarks / Reasons to Use

### Shapely Comparisons

This comparison was all 50 US states vs one in shapely, I don't how to use collections methods on geometry collections in shapely and I don't feel like learning how to make a collection in the first place and I doubut it would get faster so I just did 1.

Its worth nothing shapely would just iterate through each polygon doing ray-casting SO IT WOULD GET SLOWER ANYWAY.

The index also contains every single area given instead of what would be a point comparison against 50 diffent geometries in a typical geometric comparison. Benchmarks in comparison against shapely:

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

### Random Benchmarks on some Example Philly Data

This data was actually collected from a philly bus API so it would be a real world test case. I didn't have any modules that I know of that I could really benchmark against this. (sorry)

```
1.92039895058 seconds to add to dataframe
0.608725070953 to map the points to geohashs
597722 points 
```

The difference here occurs from the fact a dataframe to be created to add the columns to the input table as were return 3 fields per one mapped field.

### Reasons to use ult

Ult has several advantages I think makes it worth looking at.

1) Its fast, faster than anything I've seen at least
2) Its relatively simple in implementation but yields an index that still represents areas or lines.
3) Lines output being the lineid, the distance along the line, as well as the percentage along the line is something I've never seen done on an algorithm like this.
4) higher level polygon indexs can easily be related to the lowest smallest area hiearchy so that area_index can yield 6 different polygon indexs from the hierarchy that can easily be created. 

To create a hiearchy simply apply your lower geohash hiearchy ultindex to your next step up index until you get to the end. This actually has an added benefit I didn't even thing of, many libraries that utitilize ray casting and then represent hiearchy to drill down find themselves in situations where they have to clip alot of areas to complete there hiearchy correctly. This operation in of itsself is super complex annoying, my indexs complete negate needing to do this by mapping the previous index to the next one up. 
In other words I go completely backwards from traditional ray-casting hierachy algs but can still yield every single output corresponding to all the higher level geohashs at that lowest layer hiearchy. I guess hiearchy isn't something thats required like in other algs, it just happens.

That being said I would be remiss if I didn't tell you the biggest problem with ult currently, large dictionary objects on an 8 gb macbook pro. While these algs. are sort of intended for servers where memory constrictions aren't really a thing, I still exceed my memory by the time I get to something as granular as zipcodes.

I'm currently experimenting with cache systems to cache part of the index without reading in the entire thing. Things like extrema slicing could remove a large majority of the areas if your only workign in a certain locality.


# How is the ultindex created?
## Polygons

The polygon index creation process is complex but I'll try to be as explicit as possible about how its created. The make_polygon_index() accepts a dataframe thats structured in a very specific manner (see example) that accurately depicts the hierarchies within each area, I may create a repo that does this for all geospatial filetypes but currently I only have one for kmls here. Although it shouldn't be hard to replicate on geojson or any other file type.
Essentially the output is df table that contains ring hiearchy about specific areas, nothing crazy, just strictly required to create an ultindex.

So the question now is how do you create an index for each area? This relies on a property that I read in some white paper a while back that states that an areas complexity is related to/proportional to the perimeter of the area, meaning we can get a specific amount of complexity or resolution of are index for a particular area by requiring a minimum amount of blocks/squares or geohashs within a perimeters alignment. So by increasing are geohash precision (making smaller blocks) and then looking at the size of the geohashs that make up the alignment we are effecitively ensuring a minimum resolution for the index. Hopefully the pictures below convey the concept I'm trying to explain.

So now we have a minimum geohash size we can get the bulk of the algorithm by taking the top left geohash block of the extrema and the bottom right and using a function I created from the repo geohash logic we can produce a grid of all geohashs within an extrema. 

This is where the most complex components of the process begin so I'll just summarize: a collision alg. is applied to get remove the low hanging fruit of geohashs that are definately outside of the area, then ray-casting is applied to every single geohash to remove any remaining geohashs that are hidden by complex geometries. Yielding the minimum geohashs within the desired area. Again, very abridged version of whats happening.

After this we can do a simple groupby truncating the last byte off each string and if the size of that df is 32 (the size of a block hierarchy) we can add just the truncated geohash instead of all 32 if not all geohashs are added. This is compiled into a dataframe and appened to a progess h5 file. 

After all the areas have been compiled the minimum and maximum geohash sizes are found and all resultant dataframes are compiled about the minimum size. 

If this is confusing I understand, but taking a look at the mainline function for relating a geohash to a polygon might help below

```python
# maps one geohash to what it needs to be
def one_polygon_index(ghash):
	global minsize
	global maxsize
	global ultindex
	global areamask

	current = minsize
	while current < maxsize:
		output = ultindex.get(ghash[:current],'')
		# logic for continuing or not
		if output == 'na':
			current += 1
		elif output == '':
			return ''
		else:
			return areamask.get(output,'')
```

As you can see a geohash is sent in if the resultant is 'na' that means the geohash has areas lower in the hiearchy and to drill down another level until either an area is found or '' string is returned indicating no area within the ultindex corresponding to the given geohash. The global instances of variables are used instead of inputs because this function is mapped to a dataframe.

## Linestring 

The linestring index is a bit harder to explain as instead of the algorithm being straight forward, judgement and preference about how you would like to apply geohashs to the different linestrings have to be considered. For example the lines index has probably been overwritten 5 times with widely varying implementations, some of which you can still see the reminents of via functions. I decided with the current iteration of the line index based on a few factors that I observed / realized. 

At first I was just doing geohashs at size 8 (which is probably 120 ft) and let the chips fall where they may for itersections. Obviously, that was lacking so I began considering other impementations, however I was always considered about the size in memory blowing up using level 9 geohashs. (I still am) This got me thinking about a system in which the itersection geohash size 8s are drilled down to while non collision size 8s could remain that size. I also decided to use a level 8 key with a '-u' to represent all the points that made up the most of the line. By doing this little manipulation we saved space in the index and ensured all level 9 geohashs within the level 8 square would be occupied. 

There were some other things I tried to do like control the hiearcharchy of pure alignments and neighbor alignments. (i.e. a pure alignment should always get presidence of a neighbor obviously. None of this stuff was super hard to figure but when it was all compounded together it was a lot of complexity. I also got a little two zealous to fill in level 8 geohashs that could were very close to be along the alignment but currently were being covered by my alg. 

The conclusion slowly became apparant after I observed a few basic things I felt to be true about GPS alignments. For one GPS alignments are generally pretty clean, and more importantly the greater lengths you have to go acquire a point the higher likelihood it has negative value or not a point for the line we wanted. In other words the harder you tried to catch them all, the less valuable the points you had became. In other words points we know are on alignments are much more indicitive of the traffic patterns were trying to observe then the outliers that may or may not be apart of the line were looking at. So a focus on quality data was made, after building algorithms that contained some frankly insane dataframe operations to get the blocks the way I desired. 

Another factor heavily heavily influenced the current decision that exists now for the line_index algorithm. [Kyoto](http://github.com/murphy214/kyoto) a project initially intended to localize points along a line (i.e. distance along a line) worked but it was rather slow and used a wierd dictionary structure that had  to be compiled just to get the distances. Considering that I was already pretty sure I was going to take a more conservative approach I had a realization that I could derive distance directly through the geohashs now on the same index!  The granularity of a level 9 geohashs is something like 15 ft. more than we'd probably ever want for measuring traffic flow, therefore getting each geohashs distance along the alignment was entirely feasible if not logical. So the final line_index currently returns 3 fields distance along a line, the lineid, and percentage along the line. Losing little if no speed. 

If your wondering the final geohash schema I came up within was absolute alignment and absolute neighbor of courese giving priority to then neighbor. I have a feeling this won't be the last time I'll change this, things are just much more simple with 1 index. Say if I had a mixed set of geohashs that couldnt give us contious distances along the alignments therefore requiring an another entire index just for distances. 

As far as implemenation and creating is considered the functions are mirrors of the points section (i.e. make_line_index and line_index(data,ultindex) to map line ids to points) and hovers around the same performence if not better than the polygon index. 

**Kyoto will now be used to transfrom this point data that will be output into the most useful datastucture I can thing of for traffic / infastructure problems. These algorithms assuming lines would have its own instance for a given city are pretty close to scaling for real-time traffic.** 

# Examples 
# Polygon Usage Example

### Writing an h5 output
The following example uses the 'states_total.csv' which can be found in this repo. 

```python
import pandas as pd
import ult

# reading in the example states csv file
# with the correct hiearchy syntax
data = pd.read_csv('states_total.csv')

# creating the ultindex h5 file 
ult.make_polygon_index(data,'states.h5')
```

### Relating the output Ultindex to points

The next example takes the h5 file created and a csv file (random_points.csv) of dataframe containing randomly generated points (lat/long) and maps it to the created index.

```python
import ult
import pandas as pd

# reading in random point csv file
# this df has two fields: [LAT and LONG]
data = pd.read_csv('random_points.csv')

# adding the geohash column to the dataframe
# the precision of the geohash is consequential to performence
# in this case were using the precision of 12 
# in reality it doesnt matter as long as your precison
# is greater then or equal to the maxsize of the index
data = ult.map_table(data,12,map_only=True)

# reading the h5 file into memory
ultindex = ult.read_h5('states.h5')

# now mapping the the ultindex using the information 
# contained in the data dataframe
data = ult.area_index(data,ultindex)

# slicing only the point areas that were found
data = data[data['AREA'].astype(str).str.len() > 0]

print data
'''
OUTPUT:
              LAT        LONG       GEOHASH            AREA
0       48.089478  -93.750921  cbt3ddktt0qk       Minnesota
1       41.379246 -102.953335  9xqg3hzet6te        Nebraska
9       38.548906 -122.784367  9qbemcx5suuk      California
13      36.818091 -100.050876  9y8c7q90vtef        Oklahoma
20      37.413120  -94.186330  9ythbdze5rn4        Missouri
21      31.053591  -97.402089  9vd8xt1uqbf5           Texas
24      38.206359  -93.041011  9yvc6g385m9c        Missouri
25      45.455005  -94.609907  cbhdx4g4t7hh       Minnesota
28      46.870968 -117.590168  c2k6sht7nmxe      Washington
31      38.144311 -117.411059  9qu8bpzysnbu          Nevada
34      26.836894  -99.089117  9uc89jt80pxk           Texas
35      41.297582 -108.241780  9x74bvuxzer1         Wyoming
37      48.329405 -109.694491  c89fzy4gjn5k         Montana
39      33.392247  -96.339963  9vgmzwz7unj7           Texas
40      46.189429 -110.379100  c81wb282ut97         Montana
41      41.284224  -92.552133  9zq4vgtd4cpx            Iowa
43      33.941764 -105.590139  9whcj4zt57mw      New Mexico
45      30.025959  -82.955948  djm4b613fk3y         Florida
51      44.436350 -108.419526  9xfuuc7k9bn3         Wyoming
52      38.544225 -107.390838  9wgek2b2kh71        Colorado
53      37.161598  -81.516280  dnw53j19uzzy        Virginia
54      45.059698 -105.257990  c8j0kfc7z0d3         Montana
56      35.134067  -78.141553  dq0rvggncbg5  North Carolina
58      43.789761  -99.213492  9zc3n7dvy546    South Dakota
62      33.539471  -89.381152  djbqy1dts4w1     Mississippi
67      43.246302  -92.687529  9zwn4bepbp14            Iowa
69      45.177513 -116.541798  c2j1h02f56bb           Idaho
74      45.228663  -99.014039  cb196cshj9v4    South Dakota
80      43.500666 -110.228819  9x9x7xjcdrg5         Wyoming
81      40.523553  -83.452706  dphwscpdg1b5            Ohio
...           ...         ...           ...             ...
499947  35.421860  -81.665577  dnmct8kgq1k4  North Carolina
499952  28.083702  -98.556564  9uczv23r3te0           Texas
499957  38.334317  -77.553863  dqbf54x18486        Virginia
499958  39.703135  -80.271515  dpncv7spd0uh   West Virginia
499961  31.590770  -91.625442  9vwgenbms6q3       Louisiana
499964  33.090380  -88.253559  djchpxztrjbu         Alabama
499965  29.758773  -95.962625  9v7c236jkv2d           Texas
499966  37.459543 -117.099734  9qstp769dkvu          Nevada
499968  42.820795 -108.558834  9xdg9emc5ydr         Wyoming
499969  45.995796  -68.105952  f2ptdjqbbmch           Maine
499974  37.802177  -96.779052  9yepj9xtsss8          Kansas
499976  46.637673 -104.575035  c8m9k64u1h3f         Montana
499977  39.991365 -109.969346  9x1g984y8db8            Utah
499978  41.834374 -108.453019  9x6vupdyvkx7         Wyoming
499979  30.095319  -85.659142  dj754y8vvmcz         Florida
499980  31.872310 -102.342257  9txjr4htst4j           Texas
499983  46.408355  -90.466860  cbr8j2kh327e       Wisconsin
499984  40.862529 -115.929773  9rm83yy92rrv          Nevada
499985  41.278137  -92.395107  9zq6c6rytfw2            Iowa
499987  36.780333  -76.100634  dq9chr9hywyn        Virginia
499988  29.312756  -97.970709  9v4qf85zjj8y           Texas
499989  33.345653 -100.084029  9vbvdw3h905p           Texas
499990  45.729267 -106.524539  c8hhpuz0q34u         Montana
499991  34.240690 -100.781937  9y06f9kf6xjp           Texas
499992  30.274349 -102.445957  9trhhz30u5wn           Texas
499993  34.624113 -109.412432  9w4hyr0k1d99         Arizona
499994  40.431791  -80.862371  dpnqpbmu60jt            Ohio
499996  46.079738 -120.169030  c24w1kx9ftsq      Washington
499997  41.527151  -86.241824  dp6sjxb95vgn         Indiana
499999  35.673768 -120.603379  9q64zw5qc86b      California

[215820 rows x 4 columns]
'''
```


