#### Polygon Index / Linstring Index for fast Spatial aggregations

Geospatial problems are hard, especially when having to relatee a set of geometries (lines or polygons) to a substantial set of points, traditional methodologies essentially are infeasible over 100k + points without huge computing time lost. The solution is pretty simple remove geometry entirely from the operation. Like many things in CS this trades precomputation time of the indexs with the performence boost of using said indicies. 


##### Pictures
![](https://cloud.githubusercontent.com/assets/10904982/19921148/0080300e-a0b3-11e6-9395-add3308f2238.png)
![](https://cloud.githubusercontent.com/assets/10904982/19921150/0395f56c-a0b3-11e6-93fe-00166d12e08f.png)
![](https://cloud.githubusercontent.com/assets/10904982/19921175/19c1e990-a0b3-11e6-8284-3563b183ac17.png)

What is ult?

Ult solves traditional geospatial problems in a way that relies heavily on the fastest localization method that I'm aware of for 2-d objects: geohashing. 

Geohashing is way of taking a 2-d object (lat/longs) and turning it into a 1-d string that importantly displays/conveys hiearchy. Ult takes the following geohash repo in python written in C++ that can geohash approximately 400k points / second and geospatially relates a point to a specific object(s) being either lines or polygons. 

While the geohashing process may be slowish this type of localization allows for multiple indexs that use geohashing to work seamlessly, and as fast as possible. 

In other words, assuming no memory constraints, any ultindex applied to a set of points should run about the same speed no matter how many polygons or lines is within the index. For example you could have an ultindex for the US states (50 polygon), and another one for zipcodes (35k polygons) and applying each of these indexs to a set of 1 millions points would return results in 1 second for each ultindex. You could also apply an index for road or other lines at basically the same speed. 

#### What is an ultindex object?
An ultindex is a deepdish h5 file that contains the following data structures:
* alignmentdf - a pandas dataframe that was the input to make the ultindex this object can easily be output to geojson
* ultindex - a flat dictionary that utilizes the geohashs hierarchy that is assembled slightly different depending on whether the index represents lines or polygons 
* areamask - a dictionary mask that saves bytes or size on the large ultindex by having a key value structure for area or line ids to a hexidecimal range so a hexidecimal number can be used in the heavily repeated ultindex instead of a long id or area. 
* metadata - contains information about the type of ultindex that is being used 

This data structure can be directly sent into a localization algorithm and determine how the localization algorithm needs to be applied based on the metadata. It can also be used to easily output/style the data structure your aggregating against and output to a geojson file.

#### How is the ultindex created?
Polygons

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

