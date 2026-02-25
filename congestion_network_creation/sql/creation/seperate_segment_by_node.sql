-- Useful query to add nodes between segments to create a new segment
-- should make into a function...

WITH nodes_to_add AS (
select 30439358 AS nodes ) -- node to add, remember still need to add the int equivalent as well

, find_affected_segments AS (
SELECT distinct segment_id, start_vid, end_vid
FROM congestion.temp_network_links_24_4
inner join here.routing_streets_24_4 using (link_dir)
inner join nodes_to_add on source = nodes or target = nodes
)
, construct_new_sets AS (
SELECT row_number() over() + (select max (segment_id) from congestion.temp_network_links_24_4) 
as new_segment_id, new_Sets.*
from 
(SELECT start_vid, nodes as end_vid
from find_affected_segments
cross join nodes_to_add
union
select nodes as start_vid, end_vid
from find_affected_segments
cross join nodes_to_add) new_Sets)

, results AS(
	SELECT results.*, new_segment_id, link_dir, start_vid, end_vid, routing_grid.id, routing_grid.geom
	FROM construct_new_sets
	, LATERAL pgr_dijkstra('SELECT id, source::int, target::int,
						   st_length(st_transform(geom, 2952)) as cost
						   FROM here.routing_streets_24_4 routing_grid
						   ',
				start_vid, end_vid) results
	INNER JOIN here.routing_streets_24_4 routing_grid ON id = edge
)

-- Insert new segments
, insert_links as (
insert into congestion.temp_network_links_24_4
select new_segment_id, start_vid, end_vid, link_dir , geom, cost as length
from results)

, insert_segments as (
insert into  congestion.temp_network_segments_24_4
select new_segment_id, start_vid, end_vid, ST_linemerge(ST_union(geom)) , sum(cost), false
from  results
group by new_segment_id, start_vid, end_vid
)
-- Delete outdated segments
, delete_old_links as 
(delete from congestion.temp_network_links_24_4
where segment_id in (select segment_id from find_affected_segments))

delete from congestion.temp_network_segments_24_4
where segment_id in (select segment_id from find_affected_segments)