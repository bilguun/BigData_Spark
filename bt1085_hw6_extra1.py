import time, datetime, operator
import sys
from pyspark import SparkContext

##########################################################################
## author: bt1085@nyu.edu
##########################################################################
## TO RUN PLEASE USE FOLLOWING COMMAND
## ./run_extra.sh bt1085_hw6_extra1.py SAT-Results.csv DOE-High-School-Directory-2014-2015.csv BusAvg SubAvg BusAvg.txt SubAvg.txt 10 JOB 
##########################################################################
## SANITY CHECK
##########################################################################
## Bus Averages 
#(u'B100', 475)
#(u'B103', 531)
#(u'B11', 503)
#(u'B12', 390)
#(u'B13', 397)
##########################################################################
## Sub Averages
##(u'1 2 3 A C E to Chambers St', 735)
##(u'1 2 3 F M to 14th St - 6th Ave', 391)
##(u'1 2 3 G N Q R S to Times Square-42nd St', 404)
##(u'1 2 3 G S to Times Square-42nd St', 425)
##(u'1 6 A J N Q R Z to Canal St', 452)
##(u'1 6 C E F M N R to 23rd St', 473)
##(u'1 A B C D to 125th St', 357)
##(u'1 A B C D to 59th St-Columbus Circle', 493)
##(u'1 A C to 168th St - Washington Hts', 349)
###########################################################################


def mapSchoolBus(parts):
    for line in parts:
        if not line.startswith('DBN'):
            fields = line.split(',')
            bus = []
            if len(fields) >5:
                dbn = fields[0]
                i=1
                if fields[i].find('"')>-1:
                    endName = False
		    i = i+1	
                    while not endName:
                        if fields[i].find('"')>-1:
                            endName = True
                        else:
                            i = i+1
                i = i+9
		if fields[i].find('"')>-1:      		
                    bus.append(fields[i].replace('"',''))
                    i = i+1
                    endBus = False
                    while not endBus:
                        if fields[i].find('"')>-1:
                            endBus=True
                            bus.append(fields[i].replace('"',''))
                        else:
                            bus.append(fields[i])
                        i = i+1
                else:
                    bus.append(fields[i])
                    i = i+1
                for b in bus:   
                    yield (dbn, b.strip(' \t\n\r'))

def mapSchoolSub(parts):
    for line in parts:
        if not line.startswith('DBN'):
            fields = line.split(',')
            if len(fields) >5:
                dbn = fields[0]
                i=1
                if fields[i].find('"')>-1:
                    endName = False
                    i = i+1
                    while not endName:
                        if fields[i].find('"')>-1:
                            endName = True
                        else:
                            i = i+1
                i = i+9
                if fields[i].find('"')>-1:
                    i = i+1
                    endBus = False
                    while not endBus:
                        if fields[i].find('"')>-1:
                            endBus=True
                        i = i+1
                else:
                    i = i+1
                endSub = False
                subwayStr = ''
                if fields[i].find('"')>-1:
                    subwayStr = fields[i].replace('"','')
                    i = i+1
                    while not endSub:
                        if fields[i].find('"')>-1:
                            endSub = True
                            subwayStr = subwayStr + fields[i].replace('"','')
                        else:
                            subwayStr = subwayStr + fields[i]
                        i = i+1
                else:
                    subwayStr = fields[i]
                
                for subway in subwayStr.split(';'):
                    if subway!='N/A':
                        yield (dbn, subway.strip(' \t\n\r'))

def mapMathScore(parts):
    for line in parts:
        if not line.startswith('DBN'):
            fields = line.split(',')
            dbn = fields[0]
            takerCount = fields[-4]
            avgScore = fields[-2]
            if not avgScore.find("s")>-1:
                yield (dbn, (takerCount, avgScore))

def mapBusSubScore(parts):
    for dbnScores in parts:
        dbn, scores = dbnScores
        takerScore, busSub = scores
        takerCount, avgScore = takerScore
        yield (busSub, (float(takerCount), float(avgScore)))

def reduceBusSubAvg(p1, p2):
    totalTakers = p1[0]+p2[0]
    avgScore = (float(p1[0]*p1[1] + p2[0]*p2[1])/totalTakers)
    return (totalTakers, avgScore)

def mapBusSubAvg(parts):
    for busSubAvg in parts:
        busSub, (totalTakers, avgScore) = busSubAvg

        yield busSub, int(avgScore)

if __name__=='__main__':
    if len(sys.argv)!=5:
        print "Usage:  <input1 (sat scores)> <input2 (school data)> <output bus> <ouptput sub"
        sys.exit(-1)
    sc = SparkContext(appName="Average Math Score") 
    bdnScores = sc.textFile(sys.argv[1]) \
                                .mapPartitions(mapMathScore)
    school = sc.textFile(sys.argv[2]) 

    bdnBus = school.mapPartitions(mapSchoolBus)
    bdnSub = school.mapPartitions(mapSchoolSub)
    
    busAvgScores = bdnScores.join(bdnBus)
    
    busAvgScores = busAvgScores \
                                .mapPartitions(mapBusSubScore) \
                                .reduceByKey(reduceBusSubAvg) \
                                .mapPartitions(mapBusSubAvg)
    busAvgScores.saveAsTextFile(sys.argv[3])

    subAvgScores = bdnScores.join(bdnSub)

    subAvgScores = subAvgScores \
                                .mapPartitions(mapBusSubScore) \
                                .reduceByKey(reduceBusSubAvg) \
                                .mapPartitions(mapBusSubAvg)
    subAvgScores.saveAsTextFile(sys.argv[4])





