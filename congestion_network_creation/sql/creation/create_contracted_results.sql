create table congestion.contracted_result_24_4 as 

with results as (
SELECT 
	id, 
	(source||contracted_vertices||target)::bigint[] as contracted_ver, 
	array_length((source||contracted_vertices||target)::bigint[], 1) as num_contracted_ver,
	source,
	target 
	FROM pgr_contraction(
  '
  SELECT centreline_id as id,source,target,  cost_length as cost
  from congestion.routing_centreline_directional

'::text,
  ARRAY[2], --linear contraction only, no deadend
   forbidden_vertices := (SELECT array_agg(distinct intersection_id) AS node_ids
    FROM congestion.network_int_px_24_4)) as a)

select segment_id, a.node_id as from_node, b.node_id as to_node, contracted_ver, num_contracted_ver, source, target
from results
left join  congestion.network_int_px_24_4 a on source = a.intersection_id
left join congestion.network_int_px_24_4 b on target = b.intersection_id
left join congestion.network_segments_24_4 on  a.node_id  = start_vid and b.node_id = end_vid;