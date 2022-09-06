-- Find out how many nodes changed in the new map version (22_2)

-- returned 2 rows
-- meaning only 2 node_id changed! 
select node_id from congestion.network_nodes
except 
select node_id from here.routing_nodes_22_2 


-- Find out how many link_dir changed in the new map version (22_2)

-- returned 288 rows
-- 288 link_dir needs to be retired 
select link_dir from congestion.network_links_21_1
except 
select link_dir from here.routing_streets_22_2 

-- Find out how many segment_id needs to be updated

-- returned 174 rows
-- 174 segment_ids needs be to retired and route
with changed_links AS (
	select link_dir from congestion.network_links_21_1
	except 
	select link_dir from here.routing_streets_22_2)
	
select distinct segment_id from  congestion.network_links_21_1 
inner join changed_links using (link_dir)


-- First find the segment_ids affected by node_id changes
-- returned 0 in this case, because those nodes were not used 
select * from congestion.network_segments
inner join (select node_id from congestion.network_nodes
			except 
			select node_id from here.routing_nodes_22_2 )a on start_vid = node_id or end_vid = node_id