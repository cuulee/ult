# Spatial Aggregations Homie

Polygon Index for spatial aggregations

Geospatial problems are hard, especially when having to relatee a set of geometries (lines or polygons) to a substantial set of points, traditional methodologies essentially are infeasible over 100k + points without huge computing time lost. The solution is pretty simple remove geometry entirely from the operation. Like many things in CS this trades precomputation time of the indexs with the performence boost of using said indicies. 

So something to keep in mind is typical geometric comparisions might make me feel good, aren't really fair when comparing it to indexing a dictionary a few times at the maximum. This algorithm takes an area or set of area (which can be complex) and creates a multi-dimmensional dictionary of different levels. For each area in an index in contains a 2-hash level precision meaning it carries to levels of geohashs within the dictionary. To decide which size is optimal rough approximations are done to hopefully get a dense enough area covered by the geohash so that the error is considerabily negligable, +/- 3 % of the area max but usually under 1 %. (I should calculate it.)

![](https://cloud.githubusercontent.com/assets/10904982/18169911/3bf81a7a-702a-11e6-846d-45b3841b48ca.png)

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
**Its also worth noting that this is a one polygon comparison ult can have as many polygons within its index as it needs and see little hit in performence, while I'm not sure how you would do with out a hard for loop through every geometry in shapely**
