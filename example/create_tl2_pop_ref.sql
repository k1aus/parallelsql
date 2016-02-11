select
        gid as tl2gid,
        count as lspop_count_ref,       
        sum as lspop_sum_ref
from (  

select  
        map.gid,
        (ST_SummaryStats(ST_Union(ST_Clip(lspop.rast,1,map.geom,true)),TRUE)).*

from  
        tl2_clean as map, public.lspop2006 as lspop
WHERE
        (1=1) and ST_Intersects(map.geom,lspop.rast) 
GROUP by gid ) sub1
