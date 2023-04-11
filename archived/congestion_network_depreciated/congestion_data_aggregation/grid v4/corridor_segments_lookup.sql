create materialized view congestion.corridor_segments_v4 as 
select int2int.segment_id as corridor_id, segments.segment_id, link_dir, int2int.length, int2int.geom 
from (select segment_id, unnest(link_set), length, geom from congestion.route_int2int3 
		)int2int
inner join here.routing_streets_18_3 on unnest = id 
inner join congestion.segment_links_v4 segments using (link_dir)