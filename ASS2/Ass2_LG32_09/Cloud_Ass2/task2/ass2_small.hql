-- This is the script
-- To run it from command line use:
--     hive -f ass2_small.hql -hiveconf db_name=lzha4956
-- To run it inside Hive Cli
--     set db_name=lzha4956;
--     source ass2_small.hql

use ${hiveconf:db_name};


--Create tables

CREATE TABLE IF NOT EXISTS photosmall
    (user STRING, date STRING, place STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/user/${hiveconf:db_name}/Ass2task2--small/step1';

CREATE TABLE IF NOT EXISTS durationsmall
    (user STRING, date DOUBLE, place STRING, num INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/user/${hiveconf:db_name}/Ass2task2--small/step2';
	
CREATE TABLE IF NOT EXISTS originalresultsmall
    (user STRING, dur DOUBLE, place STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/user/${hiveconf:db_name}/Ass2task2--small/step3';
	
CREATE TABLE IF NOT EXISTS resultsmall
    (user STRING, place STRING, count INT, max DOUBLE, min DOUBLE, average DOUBLE, total DOUBLE)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/user/${hiveconf:db_name}/Ass2task2--small/step4';
	
CREATE TABLE IF NOT EXISTS final1small
    (user STRING, content STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/user/${hiveconf:db_name}/Ass2task2--small/step5';
	
CREATE TABLE IF NOT EXISTS finalresultsmall
    (user STRING, duration STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/user/${hiveconf:db_name}/Ass2task2--small/finalresult';	
	
--Add UDFs

ADD jar /usr/cs2/lzha4956/RowNumber.jar;
CREATE TEMPORARY FUNCTION rownumber as 'udf.RowNumber';

ADD jar /usr/cs2/lzha4956/duration.jar;
CREATE TEMPORARY FUNCTION duration as 'udf.duration';

--Queries
INSERT OVERWRITE TABLE photosmall
SELECT a.owner, a.date_taken, split(p.place_url,"/")[1] 
FROM
	(SELECT owner, place_id, date_taken
	 FROM testdata) a
JOIN share.Place p ON p.place_id=a.place_id
WHERE p.place_type_id != 29
ORDER BY a.owner ASC, a.date_taken ASC;


INSERT OVERWRITE TABLE durationsmall
SELECT user, duration(user, date, place), place, num
FROM
	(SELECT *
	FROM
		(SELECT user, date, place, rownumber(user, place) num
		 FROM photosmall) a
	ORDER BY user ASC, date DESC) b;
	
INSERT OVERWRITE TABLE originalresultsmall
SELECT user, date, place
FROM   durationsmall
WHERE  num = 1;	

INSERT OVERWRITE TABLE resultsmall
SELECT user, place, round(count(*),1), round(max(dur),1), round(min(dur),1), round(avg(dur),1), round(sum(dur),1)
FROM   originalresultsmall
GROUP BY user, place;

INSERT OVERWRITE TABLE final1small
SELECT user, concat(place,"(",count,",",max,",",min,",",average,",",total,"), ")
from resultsmall;

insert overwrite table finalresultsmall
select user,collect_set(content) 
from final1small
group by user;			