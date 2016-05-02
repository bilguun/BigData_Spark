import datetime
import operator
from operator import add
import os
import sys
import pyspark
import heapq
 
def indexZones(shapeFilename):
    import rtree
    import fiona.crs
    import geopandas as gpd
    index = rtree.Rtree()
    zones = gpd.read_file(shapeFilename).to_crs(fiona.crs.from_epsg(2263))
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findNb(p, index, nbs):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if any(map(lambda x: x.contains(p), nbs.geometry[idx])):
            return nbs.neighborhood[idx]
    return -1

def findBr(p, index, boroughs):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if any(map(lambda x: x.contains(p), boroughs.geometry[idx])):
            return boroughs.boroname[idx]
    return -1

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

def mapToNB(parts):
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    nb_index, neighbor = indexZones('neighborhoods.geojson')
    br_index, borough = indexZones('boroughs.geojson')

    for line in parts:
        fields = line.strip().split(',')
        if len(fields)>10 and all((fields[5],fields[6],fields[9],fields[10])) and all((isfloat(fields[5]),isfloat(fields[6]),isfloat(fields[9]),isfloat(fields[10]))):
        
            pickup_location  = geom.Point(proj(float(fields[5]), float(fields[6])))
            dropoff_location = geom.Point(proj(float(fields[9]), float(fields[10])))

            pickup_nb = findNb(pickup_location, nb_index, neighbor)
            dropoff_boro = findBr(dropoff_location, br_index, borough)

            if pickup_nb!=-1 and dropoff_boro!=-1:
                yield ((dropoff_boro, pickup_nb), 1)

#def reduceAdd(p1,p2):
    


if __name__=='__main__':
    if len(sys.argv)<3:
        print "Usage: <input files> <output path>"
        sys.exit(-1)

    sc = pyspark.SparkContext()

    trips = sc.textFile(','.join(sys.argv[1:-1]))

    output = trips \
        .mapPartitions(mapToNB) \
        .reduceByKey(add);
    
    output.saveAsTextFile(sys.argv[-1])

