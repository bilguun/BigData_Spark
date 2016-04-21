REGISTER /opt/cloudera/parcels/CDH-5.4.5-1.cdh5.4.5.p0.7/lib/pig/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage; 


satScores = LOAD 'SAT-Results.csv' USING CSVExcelStorage();

bdmCountScore = FOREACH satScores GENERATE $0 as bdm, $2 as counts, $4 as score;

bdmCountScore = FILTER bdmCountScore BY score != 's';

bdmCountScore = FOREACH bdmCountScore GENERATE 
								bdm, (float)counts, (float)score, (float)counts*(float)score as totals;


-----------------SANITY CHECK------------------------
--(Bronx,430)
--(Queens,472)
--(Brooklyn,460)
--(Manhattan,492)
--(Staten Island,476)
-----------------------------------------------------

--SCHOOL DATA
schools = LOAD 'DOE-High-School-Directory-2014-2015.csv' USING CSVExcelStorage();

bdmBoro = FOREACH schools GENERATE $0 as bdm, $2 as boro;

bdmBoroCountScore = JOIN bdmCountScore by bdm, bdmBoro by bdm;

bdmBoroCountScore = FOREACH bdmBoroCountScore GENERATE 
									bdmBoro::boro as boro, 
                            		bdmCountScore::counts as counts,
                            		bdmCountScore::score as score,
                            		bdmCountScore::totals as totals;

groupByBoro = GROUP bdmBoroCountScore BY (boro);

byBoroScore = FOREACH groupByBoro GENERATE
    group AS boro,
    SUM(bdmBoroCountScore.counts) as counts,
    SUM(bdmBoroCountScore.totals) as totalScore;
    

byBoroScore = FOREACH byBoroScore GENERATE boro, (int)totalScore/(int)counts;

dump byBoroScore;
