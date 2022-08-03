create table congestion.segments_centreline_routed_21_1_missing as

-- all segments with missing int_id info
WITH missing_segment AS (
	select 		segment_id, start_vid, end_vid, fnode.int_id as start_int, tnode.int_id as end_int
	from 		congestion.network_segments
	left join 	congestion.network_int_px_21_1 fnode on fnode.node_id = network_segments.start_vid
	left join 	congestion.network_int_px_21_1 tnode on tnode.node_id = network_segments.end_vid
	where  		(fnode.int_id is null or tnode.int_id is null) 
)
-- nodes with no int_id matches
, missing_int AS (

	select 		node_id, int_id
	from 		congestion.network_int_px_21_1 
	where 		int_id is null
)
-- finding segments that are connected to the node with no int_id matches
, matches AS (
	select a1.segment_id as first_seg, a2.segment_id as sec_seg, a2.start_vid, a1.end_vid, a2.start_int, a1.end_int
	from missing_int m
	left join missing_segment a1 on m.node_id = a1.start_vid
	left join missing_segment a2  on m.node_id = a2.end_vid
	where a2.start_vid != a1.end_vid)
	
select ARRAY[first_seg, sec_seg] as segment_set, start_vid, end_vid, start_int, end_int, 
		array_agg(geo_id) as geo_id_set, 
		ST_linemerge(ST_union(a.geom)) as geom
from matches
CROSS JOIN LATERAL pgr_dijkstra('SELECT id, source::int, target::int, cost
				 	   			FROM gis.centreline_routing_20220705_undirected', 
								start_int, 
								end_int, 
								FALSE)
inner join gis.centreline_20220705 a on geo_id = edge
group by start_vid, end_vid, start_int, end_int, first_seg, sec_seg