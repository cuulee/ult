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

# Polygon Usage Example

### Writing an h5 output
The following example uses the 'states_total.csv' which can be found in this repo. 

```python
import pandas as pd
import ult

# reading in the example states csv file
# with the correct hiearchy syntax
data = ult.read_csv('states_total.csv')

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


