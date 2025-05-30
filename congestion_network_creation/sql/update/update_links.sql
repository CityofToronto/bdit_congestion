-- Create new links table except the segment_ids that 
-- are outdated

CREATE TABLE congestion.network_links_22_2 AS 

-- retired links
with changed_links AS (
	select link_dir from congestion.network_links_21_1
	except 
	select link_dir from here.routing_streets_22_2)
-- retired segments	
, changed_seg AS (
	select distinct segment_id, start_vid, end_vid 
	from  congestion.network_links_21_1 
	inner join changed_links using (link_dir))
-- everything in network_links other than the retired segments
select      network_links_21_1.* 
from 		congestion.network_links_21_1
left join 	changed_seg using (segment_id)
where 		changed_seg is null 


-- Insert routed results using new map version (22_2)
-- for retired segments

-- retired links
with changed_links AS (
	select link_dir from congestion.network_links_21_1
	except 
	select link_dir from here.routing_streets_22_2)
-- retired segments		
, changed_seg AS (
	select distinct segment_id, start_vid, end_vid 
	from  congestion.network_links_21_1 
	inner join changed_links using (link_dir))

INSERT INTO congestion.network_links_22_2 -- insert result to newly created network_links table
SELECT segment_id, start_vid, end_vid, link_dir, routing.geom, round(st_length(st_transform(geom, 2952))::numeric, 2) as length
FROM   changed_seg
CROSS JOIN LATERAL pgr_dijkstra('SELECT id, source::int, target::int, st_length(st_transform(geom, 2952)) as cost 
				 	   			FROM here.routing_streets_22_2',  -- route using new map version's routing_streets
								start_vid, 
								end_vid)
INNER JOIN here.routing_streets_22_2 routing ON id = edge;


-- Check to see if all segments are inserted 
-- returned 0 rows 
select distinct segment_id from congestion.network_links_21_1
except
select distinct segment_id from congestion.network_links_22_2

--- Check to see if both returned the same number of segment_id

-- returned 6504
select count(distinct segment_id) from congestion.network_links_21_1
-- returned 6504
select count(distinct segment_id) from congestion.network_links_22_2