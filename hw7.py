import datetime
import operator
from operator import add
import os
import sys
import pyspark
from  pyspark.sql import SQLContext
from pyspark.sql import Row
 
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
                yield (dropoff_boro, pickup_nb, 1)


if __name__=='__main__':
    if len(sys.argv)<3:
        print "Usage: <input files> <output path>"
        sys.exit(-1)

    sc = pyspark.SparkContext()

    trips = sc.textFile(','.join(sys.argv[1:-1]))

    output = trips \
        .mapPartitions(mapToNB) \
        .reduceByKey(add);
    
    sqlContext = SQLContext(sc)

    csv_data = trips.map(lambda l: l.split(","))    
    
    row_data = csv_data.map(lambda p: Row(
            boro=str(p[0]),
            nb=str(p[1]),
            cnt=int(p[2])))

    df = sqlContext.createDataFrame(row_data)

    df.registerTempTable("trips")

    topM = sqlContext.sql("select boro, nb, cnt from trips where boro='Manhattan' order by cnt desc limit 3")#.collect()
    topB = sqlContext.sql("select boro, nb, cnt from trips where boro='Brooklyn' order by cnt desc limit 3")#.collect()
    topBx = sqlContext.sql("select boro, nb, cnt from trips where boro='Bronx' order by cnt desc limit 3")#.collect()
    topS = sqlContext.sql("select boro, nb, cnt from trips where boro='Staten Island' order by cnt desc limit 3")#.collect()
    topQ = sqlContext.sql("select boro, nb, cnt from trips where boro='Queens' order by cnt desc limit 3")#.collect()
    
    topN = topM.unionAll(topB)
    topN = topM.unionAll(topBx)
    topN = topM.unionAll(topS)
    topN = topM.unionAll(topQ) 

    topN.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile(sys.argv[-1])
