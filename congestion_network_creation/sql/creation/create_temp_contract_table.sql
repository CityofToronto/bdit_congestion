create table congestion.temp_contract_24_4 as
SELECT segment_id, from_node, to_node, contracted_ver, 
num_contracted_ver, source as from_int, 
target as to_int, links as ids, geom
FROM congestion.contracted_result_24_4 results
left join lateral congestion.route_contracted_path(
    results.source,  -- start node
    results.target,  -- end node
    contracted_ver -- only allow edges that has nodes that got contracted
)a on true
where segment_id is not null;

-- insert centrelines matches, where only 1 centreline is within source and target
insert into congestion.temp_contract_24_4
select segment_id, a.node_id as from_node, b.node_id as to_node, null as contracted_ver, 
null as num_contracted_ver, a.intersection_id as from_int, b.intersection_id as to_int,
array[id] as ids, cent.geom
from congestion.network_segments_24_4
left join congestion.network_int_px_24_4 a on start_vid = a.node_id
left join congestion.network_int_px_24_4 b on end_vid = b.node_id
left join gis_core.routing_centreline_directional cent on source = a.intersection_id and target = b.intersection_id 
where id is not null
