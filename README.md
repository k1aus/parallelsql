# parallelsql
* A sql extenstion that parallizes sql queries using dblink

Author: Copyright (c) 2015 Klaus Ackermann <klaus.ackermann@monash.edu>

Written during the The Eric & Wendy Schmidt Data Science for Social Good Fellowship at University of Chicago 2015.

`parllelsql.sql` defines a single Postgres function:

	SELECT parallelsql
	(       'geo',                                  --database
	        'tl2',                          		--table
	        'map.gid',                              --variable to partition by processes    
	        '$STATEMENT',                           --the statement as string to be executed in parallel 
	        'tl2_pop_ref',                  		--result table, has to be created first
	        'map',                                  --table alias used for split column
	        16,                                     --number of cores
	        '1=1',                                  --replace string in the query
	        500                                     --block size
	); 


The function takes a single sql statement and replaces a string in a where condition to execute the query in parallel. Suitable for all queries where the input can be split into sub parts. A block size is specified to reduce the amount of memory required for storing in between results, such as it is common for long running geometric operations using PostGis.

The code is an extension of [parsel](https://gist.github.com/mjgleaso/8031067). However, `parallelsql()` does not create between results in memory and can therefore make use of database indexes. The block wise load balancing allows to reduce the runtime by querries that otherwise would require a too large cross product internally. Especially in combination with the Postgres  `LATERAL` join feature, the runtime can be reduced.

Benefits and use cases so far:
* Used on Amazon AWS Postgres instance with 28 cores and PostGIS for distance calculation between 4 Million and 50000 georeferenced points.
* Calculation of raster summary statistics for world wide population data.
* Can be run directly from a shell script and does not require a manual `Map-Reduce` implementation in a programming language.

## Example

Under `example` is a bash script and sql statement for the calculation of summary raster statistics in parallel.
