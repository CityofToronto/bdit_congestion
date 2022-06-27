-- Table created to assign centreline intersection id and/or px
-- to each nodes used in routing the network 

CREATE TABLE congestion.network_int_px_21_1 AS 

select  node_id, 
		int_id, 
		px, 
		nodes.geom as node_geom,
		ints.geom as ints_geom,
		dist
		
from congestion.network_nodes nodes
cross join lateral (select int_id, px, geom, ST_transform(ints.geom, 2952) <-> ST_transform(nodes.geom, 2952) as dist
					 from congestion.selected_intersections ints
					 order by (ints.geom <-> nodes.geom)
					 LIMIT 1)ints

order by dist desc;

COMMENT ON TABLE congestion.network_int_px_21_1
    IS 'Lookup table containing nodes used in routing network and its equivalent centreline intersection id and px. Based on 21_1 map version nodes. ';
