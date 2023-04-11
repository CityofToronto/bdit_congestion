--route segments from intersections to intersections

WITH intersections as(
	SELECT id as area_id, array_agg(ref_in_id::int) AS ints
	FROM congestion.here_intersection_nodes xsections
	INNER JOIN congestion.routing_boundary area ON ST_Contains(ST_transform(ST_Buffer(ST_Transform(area.geom,98012),15), 4326) , xsections.geom)
	GROUP BY area_id 
), results AS(
	SELECT results.*, routing_grid.id, routing_grid.geom
	FROM intersections
	, LATERAL pgr_dijkstra('SELECT id, source::int, target::int, st_length(st_transform(geom, 2952)) as cost FROM congestion.routing_grid',
				ints, ints) results
	INNER JOIN congestion.routing_grid ON id = edge
)

SELECT row_number() over () as segment_id, start_vid, end_vid, array_agg(id order by path_seq) as link_set, array_agg(cost order by path_seq) as length_set, st_linemerge(st_union(s.geom)) as geom, sum(cost) as length
into congestion.route_int2int3
FROM (select distinct path_seq, start_vid, end_vid, edge, node, cost, agg_cost, id, geom  from results)s
LEFT OUTER JOIN congestion.here_intersection_nodes ON node = ref_in_id AND node != start_vid
GROUP BY start_vid, end_vid
HAVING COUNT(ref_in_id) =0
order by start_vid, end_vid