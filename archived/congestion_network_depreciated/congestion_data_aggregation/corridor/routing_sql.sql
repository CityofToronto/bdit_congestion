with int as (
	select * from (																		   
	select ref_in_id from (select distinct link_id from congestion.corridor_routing_cleaned)a
	inner join here_gis.streets_att_18_3 using (link_id)
	union all
	select nref_in_id  from (select distinct link_id from congestion.corridor_routing_cleaned)b
	inner join here_gis.streets_att_18_3 using (link_id)
				 )a
group by ref_in_id											
having count(ref_in_id) >=3 or count(ref_in_id) = 1)
, int_set as (select distinct ref_in_id, geom 
from int inner join here_gis.zlevels_18_3 on node_id = ref_in_id
union all
select node_id, geom
from here_gis.zlevels_18_3 where node_id in (30326082,30362000))

,intersections as(
	SELECT id as area_id, array_agg( distinct ref_in_id::int) AS ints
	FROM int_set xsections
	INNER JOIN congestion.corridor_boundary area ON ST_Contains(area.geom, xsections.geom)
	GROUP BY area_id 
), results AS(
	SELECT results.*, routing_grid.id, routing_grid.geom
	FROM intersections
	, LATERAL pgr_dijkstra('SELECT id::bigint, source, target, length::int as cost FROM congestion.corridor_routing_cleaned',
				ints, ints) results
	INNER JOIN congestion.corridor_routing_cleaned routing_grid ON id = edge
)

SELECT row_number() over () as corridor_id, start_vid, end_vid, array_agg(id order by path_seq) as link_set, array_agg(cost order by path_seq) as length_set, st_linemerge(st_union(s.geom)) as geom, sum(cost) as length
into congestion.route_corridor
FROM (select distinct path_seq, start_vid, end_vid, edge, node, cost, agg_cost, id, geom  from results)s
LEFT OUTER JOIN int_set ON node = ref_in_id AND node != start_vid
GROUP BY start_vid, end_vid
HAVING COUNT(ref_in_id) =0
order by start_vid, end_vid
