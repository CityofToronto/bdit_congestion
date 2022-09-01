--route segments from node to node with many-many


-- group nodes using network_routing_boundary
-- in order to reduce routing a large amount of node at one go 
-- which will lead to memory issues
WITH intersections as(
	SELECT id as area_id, 
            array_agg(node_id::int) AS ints
	FROM congestion.network_nodes xsections
	INNER JOIN congestion.network_routing_boundary area ON ST_Contains(ST_transform(ST_Buffer(ST_Transform(area.geom,98012),15), 4326) , xsections.geom)
	GROUP BY area_id ), 

-- route using a semi-cleanded network_routing_grid
-- got rid of most ramps and irrelavant link_dirs (manually on QGIS)
-- use geometry derived length as cost
-- routed using many-to-many
results AS(
	SELECT results.*, routing_grid.id, routing_grid.geom
	FROM intersections
	, LATERAL pgr_dijkstra('SELECT id, source::int, target::int, st_length(st_transform(geom, 2952)) as cost FROM congestion.network_routing_grid',
				ints, ints) results
	INNER JOIN congestion.network_routing_grid routing_grid ON id = edge)

-- create segment_id using row_number
-- aggregate link_dirs into an array, as well as length
-- insert into a result table for further cleaning
SELECT row_number() over () as segment_id, 
        start_vid, 
        end_vid, 
        array_agg(id order by path_seq) as link_set, 
        array_agg(cost order by path_seq) as length_set, 
        st_linemerge(st_union(s.geom)) as geom, 
        sum(cost) as length
        
INTO congestion.network_routing_results
FROM (select distinct path_seq, start_vid, end_vid, edge, node, cost, agg_cost, id, geom  
      from results)s
LEFT OUTER JOIN congestion.network_nodes ON node = node_id AND node != start_vid
GROUP BY start_vid, end_vid
HAVING COUNT(node_id) =0 -- exclude routed results that went pass any other node_ids
order by start_vid, end_vid
