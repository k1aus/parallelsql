#!/bin/bash -x

RUN_ON_MYDB="psql -X -U postgres -h geoserver geo"


$RUN_ON_MYDB <<SQL
drop table if exists tl2_pop_ref;
SQL

#run the statement to create a table with the correct column but no content 
TABLE=$(echo 'create table tl2_pop_ref as'; cat create_tl2_pop_ref.sql; echo 'limit 0')
$RUN_ON_MYDB -c "$TABLE"

STATEMENT=$(cat create_tl2_pop_ref.sql)


#calculate the raster statistics in parallel
$RUN_ON_MYDB <<SQL
SELECT parallelsqlload
(	'geo',				--database
	'tl2',		                --table
	'map.gid',			--variable to partition by processes	
        '$STATEMENT',			--the statement to executed in parallel 
	'tl2_pop_ref',			--result table, has to be created first
	'map',				--table alias used for split column
	16,				--number of cores
	'1=1',				--replace string in the query
	500	 			--block size
);			
SQL

#terminate all db_link conections if the script was interrupted just in case
$RUN_ON_MYDB <<SQL
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'geo'
  AND client_addr is null;
SQL
