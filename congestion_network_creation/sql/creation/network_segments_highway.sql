-- initial pass of highway selections

UPDATE  congestion.network_segments
SET     highway = TRUE
FROM (	select 			distinct segment_id  as seg
		    from 			  congestion.network_links_21_1 a
  		  inner join 		here.routing_streets_21_1 using (link_dir)
  		  inner join 		here_gis.streets_att_21_1 using (link_id)
  		  where 			  st_name LIKE 'HWY-409' or 
                      st_name LIKE 'HWY-401%' or  
                      st_name LIKE  'GARDINER EXPY' or 
                      st_name LIKE 'HWY-427%' or 
                      st_name LIKE 'DON VALLEY%' or
                      st_name LIKE 'HWY-404' or 
                      st_name LIKE 'ALLEN %' or 
                      st_name LIKE 'HWY-27' or
                      st_name LIKE 'HWY-400' or 
                      st_name LIKE 'HWY-2A' or 
                      st_name LIKE 'HWY-401') hwy
WHERE   segment_id = seg 