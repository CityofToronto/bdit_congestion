-- Routes centreline using int_id <-> node_id look up table
-- results in centreline that makes up each segment
--SELECT 5740
--Query returned successfully in 30 min 36 secs.

CREATE TABLE congestion.segments_centreline_routed_21_1 AS 

WITH segment_info AS (
	select 		segment_id, start_vid, end_vid, fnode.int_id as start_int, tnode.int_id as end_int
	from 		congestion.network_segments
	left join 	congestion.network_int_px_21_1 fnode on fnode.node_id = network_segments.start_vid
	left join 	congestion.network_int_px_21_1 tnode on tnode.node_id = network_segments.end_vid
	where 		fnode.int_id != tnode.int_id and 
				fnode.int_id is not null and 
				tnode.int_id is not null)
, result AS (
	SELECT * 
	FROM   			   segment_info
	CROSS JOIN LATERAL pgr_dijkstra('SELECT id, source::int, target::int, cost
				 	   				 FROM gis.centreline_routing_20220705_undirected', 
									 start_int, 
									 end_int, 
									 FALSE))
					   
	select segment_id, start_vid, end_vid, start_int, end_int, array_agg(geo_id), ST_linemerge(ST_union(a.geom)) as geom
	from result
	inner join gis.centreline_20220705 a on geo_id = edge
	group by segment_id, start_vid, end_vid, start_int, end_int
