-- This is the script
-- To run it from command line use:
--     hive -f ass2_hive.hql -hiveconf db_name=lzha4956
-- To run it inside Hive Cli
--     set db_name=lzha4956;
--     source ass2_hive.hql

use ${hiveconf:db_name};


--Create tables

CREATE TABLE IF NOT EXISTS photo
    (user STRING, date STRING, place STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/user/${hiveconf:db_name}/Ass2task2/photoorderbyuser-date';

CREATE TABLE IF NOT EXISTS duration
    (user STRING, date DOUBLE, place STRING, num INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/user/${hiveconf:db_name}/Ass2task2/duration';
	
CREATE TABLE IF NOT EXISTS originalresult
    (user STRING, dur DOUBLE, place STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/user/${hiveconf:db_name}/Ass2task2/originalresult';
	
CREATE TABLE IF NOT EXISTS result
    (user STRING, place STRING, count INT, max DOUBLE, min DOUBLE, average DOUBLE, total DOUBLE)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/user/${hiveconf:db_name}/Ass2task2/durationresult';
	
CREATE TABLE IF NOT EXISTS final1
    (user STRING, content STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/user/${hiveconf:db_name}/Ass2task2/final1';
	
CREATE TABLE IF NOT EXISTS finalresult
    (user STRING, duration STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/user/${hiveconf:db_name}/Ass2task2/finalresult';	
	
--Add UDFs

ADD jar /usr/cs2/lzha4956/RowNumber.jar;
CREATE TEMPORARY FUNCTION rownumber as 'udf.RowNumber';

ADD jar /usr/cs2/lzha4956/duration.jar;
CREATE TEMPORARY FUNCTION duration as 'udf.duration';

--Queries
INSERT OVERWRITE TABLE photo
SELECT a.owner, a.date_taken, split(p.place_url,"/")[1] 
FROM
	(SELECT owner, place_id, date_taken
	 FROM share.Photo) a
JOIN share.Place p ON p.place_id=a.place_id
WHERE p.place_type_id != 29
ORDER BY a.owner ASC, a.date_taken ASC;


INSERT OVERWRITE TABLE duration
SELECT user, duration(user, date, place), place, num
FROM
	(SELECT *
	FROM
		(SELECT user, date, place, rownumber(user, place) num
		 FROM photo) a
	ORDER BY user ASC, date DESC) b;
	
INSERT OVERWRITE TABLE originalresult
SELECT user, date, place
FROM   duration
WHERE  num = 1;	

INSERT OVERWRITE TABLE result
SELECT user, place, round(count(*),1), round(max(dur),1), round(min(dur),1), round(avg(dur),1), round(sum(dur),1)
FROM   originalresult
GROUP BY user, place;

INSERT OVERWRITE TABLE final1
SELECT user, concat(place,"(",count,",",max,",",min,",",average,",",total,"), ")
from result;

insert overwrite table finalresult
select user,collect_set(content) 
from final1 
group by user;			