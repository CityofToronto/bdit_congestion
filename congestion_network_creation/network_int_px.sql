-- Table created to assign centreline intersection id and/or px
-- to each nodes used in routing the network 

CREATE TABLE congestion.network_int_px_21_1 AS 

SELECT  node_id, 
		int_id, 
		px, 
		nodes.geom as node_geom,
		ints.geom as ints_geom,
		dist
		
FROM congestion.network_nodes nodes
CROSS JOIN LATERAL ( SELECT int_id, px, geom, ST_transform(ints.geom, 2952) <-> ST_transform(nodes.geom, 2952) AS dist
					 FROM congestion.selected_intersections ints
					 ORDER BY (ints.geom <-> nodes.geom)
					 LIMIT 1)ints
WHERE dist < 25
ORDER BY dist desc;

COMMENT ON TABLE congestion.network_int_px_21_1
    IS 'Lookup table containing nodes used in routing network and its equivalent centreline intersection id and px. Based on 21_1 map version nodes. ';
