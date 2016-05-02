import datetime
import operator
from operator import add
import os
import sys
import pyspark
import heapq
from  pyspark.sql import SQLContext
from pyspark.sql import Row



if __name__=='__main__':
    if len(sys.argv)<3:
        print "Usage: <input files> <output path>"
        sys.exit(-1)

    sc = pyspark.SparkContext()

    trips = sc.textFile(','.join(sys.argv[1:-1])).cache()

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
    topM = topM.unionAll(topB)
    topM = topM.unionAll(topBx)
    topM = topM.unionAll(topS)
    topM = topM.unionAll(topQ)	
    topM.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("file22.csv")


