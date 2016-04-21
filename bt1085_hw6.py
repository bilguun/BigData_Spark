import time, datetime, operator
import sys
from pyspark import SparkContext

###################################################################
#### author: bt1085@nyu.edu
###################################################################
#### Run command using .run.sh
#### ./run.sh bt1085_hw6.py SAT-Results.csv DOE-High-School-Directory-2014-2015.csv AvgSat MathAveByBoro.txt 3 Sat
###################################################################
## Result
##(u'Bronx', 430)
##(u'Brooklyn', 460)
##(u'Manhattan', 492)
##(u'Queens', 472)
##(u'Staten Island', 476)
##################################################################3


def mapSchoolBoro(parts):
    for line in parts:
	if not line.startswith('DBN'):
            fields = line.split(',')
            #Line was somehow contained extra newline characters
            #So here I am checking if line actually contains the info 		
            if len(fields) >3:
		dbn = fields[0]
		boro = fields[2]
		if fields[1].find('"')>-1:
	            i=2
                    found = False
                    # Parsing to find next field contains closing double quote
                    while not found:	
                        if fields[i].find('"')>-1:
                            boro = fields[i+1]
                            found = True
                        i = i+1 			
            	yield (dbn, boro)

def mapMathScore(parts):
    for line in parts:
        if not line.startswith('DBN'):
	    fields = line.split(',')
            dbn = fields[0]	
            takerCount = fields[-4]
            avgScore = fields[-2]
            # Checking whether if score is emtpy where it was denoted by 's'
            if not avgScore.find("s")>-1:
	        yield (dbn, (takerCount, avgScore))

def mapBoroScore(parts):
    for dbnScores in parts:
        dbn, scores = dbnScores
        takerScore, boro = scores
        takerCount, avgScore = takerScore
        yield (boro, (int(takerCount), int(avgScore)))

def reduceBoroAvg(p1, p2):
    totalTakers = p1[0]+p2[0]
    avgScore = (float(p1[0]*p1[1] + p2[0]*p2[1])/totalTakers)
    return (totalTakers, avgScore)

def mapBoroAvg(parts):
    for boroAvg in parts:
        boro, (totalTakers, avgScore) = boroAvg
    	yield boro, int(avgScore)

if __name__=='__main__':
    if len(sys.argv)!=4:
        print "Usage:  <input1 (sat)> <input2 (school)> <output path>"
        sys.exit(-1)

    sc = SparkContext(appName="Average Math Score") # do not set 'master'

    bdnScores = sc.textFile(sys.argv[1]) \
    				.mapPartitions(mapMathScore)
    bdnBoro = sc.textFile(sys.argv[2]) \
    				.mapPartitions(mapSchoolBoro)
    #Joining to sets
    boroAvgScores = bdnScores.join(bdnBoro) 
    #Calculating averages
    avgScores = boroAvgScores \
				.mapPartitions(mapBoroScore) \
				.reduceByKey(reduceBoroAvg) \
				.mapPartitions(mapBoroAvg)#.collect()

    avgScores.saveAsTextFile(sys.argv[3])
