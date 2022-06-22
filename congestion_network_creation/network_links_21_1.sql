create table congestion.network_links_21_1 as 

with temp as (
select segment_id, start_vid, end_vid, unnest(link_set) as link_uid
from congestion.network_routing_results)

select 	segment_id, 
		link_dir, 
		geom, 
		round(ST_length(st_transform(geom, 2952))::numeric,2) as length
from temp
inner join here.routing_streets_21_1 on id = link_uid
order by segment_id;

COMMENT ON TABLE congestion.network_links_21_1 IS 
'A lookup table for the congestion network, contains segment_id and link_dir (21_1 map version) lookup.';