--
-- A sql extenstion that parallizes sql queries using dblink
-- Author: Copyright (c) 2015 Klaus Ackermann  <klaus.ackermann@monash.edu>
-- Written during the The Eric & Wendy Schmidt Data Science for Social Good Fellowship at University of Chicago 2015.
--
CREATE OR REPLACE FUNCTION public.parallelsql(db text, table_to_chunk text, pkey text, query text, output_table text, table_to_chunk_alias text default '', num_procs integer default 2, replacement_string text default '1=1',blocksize integer default 1000)
  RETURNS text AS
$BODY$
DECLARE 
  sql     TEXT;
  min_id  bigint;
  max_id  bigint;
  step_size bigint;
  num_chunks bigint;
  lbnd bigint;
  ubnd bigint;
  subquery text;
  insert_query text;
  i bigint;
  conn text;
  n bigint;
  num_done bigint;
  status bigint;
  dispatch_result bigint;
  dispatch_error text;
  part text;
  rand text;
  array_procs int[];
  current_proc int;
  used_procs int;

  done int;

BEGIN
  --find minimum pkey id
  sql := 'SELECT min(' || pkey || ') from ' || table_to_chunk || ' ' || table_to_chunk_alias || ';';
  execute sql into min_id;

  --find maximum pkey id
  sql := 'SELECT max(' || pkey || ')  from ' || table_to_chunk || ' ' || table_to_chunk_alias ||';';
  execute sql into max_id;

  -- determine size of chunks based on min id, max id, and number of chunks
  --the step_size is determent by the block size
  step_size := blocksize;
  sql := 'SELECT ( ' || max_id || '-' || min_id || ')/' || blocksize || ';';
  
  EXECUTE sql into num_chunks;
  RAISE NOTICE 'Total number of chunks:  %',num_chunks;


  --initialize array for keeping track of finished processes
  sql := 'SELECT array_fill(0, ARRAY[' || num_procs ||']);';
  EXECUTE sql into array_procs;

  current_proc := 0;
  used_procs := 0;
  -- loop through chunks

  <<chunk_loop>>
  for lbnd,ubnd,i in 
	SELECT  generate_series(min_id,max_id,step_size) as lbnd, 
		generate_series(min_id+step_size,max_id+step_size,step_size) as ubnd,
		generate_series(1,num_chunks+1) as i
  LOOP

    -- create a subquery string that will be added to the where in the original query markes as 1=1
    --if the last chuck is read, add greater then and is null
    if i <> num_chunks+1 then
	     part := ' ' || pkey || ' >= ' || lbnd || ' AND ' || pkey || ' < ' || ubnd;
    else
        part := ' ' || pkey || ' >= ' || lbnd || ' OR ' || pkey || ' is NULL';
    end if;
   	
    RAISE NOTICE 'Chunk %: %', i,part;

    current_proc := current_proc + 1;
    array_procs[current_proc] = 0;
    used_procs := used_procs + 1;
    
    
    --make a new db connection
    conn := 'conn_' || current_proc; 
    RAISE NOTICE 'New Connection name: %',conn;
 
    sql := 'SELECT dblink_connect(' || QUOTE_LITERAL(conn) || ', ''dbname=' || db ||''');';
    execute sql;


    --edit the input query using the subsquery string
    sql := 'SELECT REPLACE(' || QUOTE_LITERAL(query) || ',' || QUOTE_LITERAL(replacement_string) || ',' || QUOTE_LITERAL(part) || ');'; 
    --debug RAISE NOTICE 'SQL COMMAND: %',sql;	  
    execute sql into subquery;
    
    insert_query := 'INSERT INTO ' || output_table || ' ' || subquery || ';';
    --raise NOTICE '%', insert_query;
    --send the query asynchronously using the dblink connection
    sql := 'SELECT dblink_send_query(' || QUOTE_LITERAL(conn) || ',' || QUOTE_LITERAL(insert_query) || ');';
    execute sql into dispatch_result;

    -- check for errors dispatching the query
    if dispatch_result = 0 then
	     sql := 'SELECT dblink_error_message(' || QUOTE_LITERAL(conn)  || ');';
	     execute sql into dispatch_error;
       RAISE '%', dispatch_error;
    end if;

    --check how many processors are in use right now
    if (i<>(num_chunks+1)) and  used_procs>=num_procs then
          done := 0 ;
	--repetatly check until one proc is finished to relaunch the next chunck
	  Loop
		  for n in 1..num_procs 
		  Loop
			conn := 'conn_' || n;
			sql := 'SELECT dblink_is_busy(' || QUOTE_LITERAL(conn) || ');';
			execute sql into status;
			if status = 0 THEN	
				-- check for error messages
				sql := 'SELECT dblink_error_message(' || QUOTE_LITERAL(conn)  || ');';
				execute sql into dispatch_error;
				if dispatch_error <> 'OK' THEN
					RAISE '%', dispatch_error;
				end if;

				--terminate the connection and resect the active proc counter so that the next
				--connection is started with the correct index
				RAISE NOTICE 'Process done:  %, Next Chunk to be started: %',conn,i+1;

				--disconnect the connection
				sql := 'SELECT dblink_disconnect(' || QUOTE_LITERAL(conn) || ');';
				execute sql;

				current_proc := n - 1; --as the counter gets increased at the beginning of the other loop
				used_procs := used_procs - 1;
				done := 1;
				array_procs[n]=1;
				
				exit; --terminate the loop
			END if;
		  end loop;
		if done = 1 then
			exit;
		end if;
		sql := 'select pg_sleep(0.5)';
		execute sql;
	  END loop;

    end if;
    
  end loop chunk_loop;

  -- wait until all queries are finished
  Loop
	  for i in 1..num_procs
	  Loop
		if array_procs[i]<>1 THEN
			conn := 'conn_' || i;
			sql := 'SELECT dblink_is_busy(' || QUOTE_LITERAL(conn) || ');';
			execute sql into status;
			if status = 0 THEN	
				-- check for error messages
				sql := 'SELECT dblink_error_message(' || QUOTE_LITERAL(conn)  || ');';
				execute sql into dispatch_error;
				if dispatch_error <> 'OK' THEN
					RAISE '%', dispatch_error;
				end if;
				used_procs := used_procs - 1;

				--disconnect the connection
				sql := 'SELECT dblink_disconnect(' || QUOTE_LITERAL(conn) || ');';
				execute sql;

				RAISE NOTICE 'Process done:  %',conn;
				array_procs[i]=1;
			END if;
		END if;
	  end loop;
	  --if num_done >= num_procs then
	  if used_procs <= 0 then
		exit;
	  end if;
	  --pause in poling
	  sql := 'select pg_sleep(1)';
	  execute sql;

  END loop;


  RETURN 'Success';

-- error catching to disconnect dblink connections, if error occurs
exception when others then
  BEGIN
  RAISE NOTICE '% %', SQLERRM, SQLSTATE;
  for n in 
	SELECT generate_series(1,i) as n
  LOOP
    	
    conn := 'conn_' || n;

    -- cancel a previous crashed query
    sql := 'SELECT dblink_cancel_query(' || QUOTE_LITERAL(conn) ||');';	
    execute sql;
    	

    sql := 'SELECT dblink_disconnect(' || QUOTE_LITERAL(conn) || ');';
    execute sql;
  END LOOP;
  exception when others then
    RAISE NOTICE '% %', SQLERRM, SQLSTATE;
  end;
  
END
$BODY$
  LANGUAGE plpgsql STABLE
  COST 100;

