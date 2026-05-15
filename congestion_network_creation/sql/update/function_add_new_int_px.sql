'''
    insert into congestion.temp_int_px_nodes_24_4
select * from congestion.add_new_int_px(30356897, '24_4', '20260119')
'''
--drop 
CREATE OR REPLACE FUNCTION congestion.add_new_int_px(nodes numeric, new_ver_id TEXT, centreline_ver text)
RETURNS TABLE (
    px text,
    px_geom GEOMETRY,
    intersection_id INT,
    int_geom GEOMETRY,
    node_id bigint ,
    node_geom GEOMETRY
) AS $$
DECLARE
    nodes_table text := 'temp_congestion_nodes_' || new_ver_id;
    centreline_name text := 'intersection_'|| centreline_ver; 

BEGIN
       
    RETURN QUERY  EXECUTE format(		
		'SELECT DISTINCT 
            signals.px,
            signals.px_geom,
            ints.intersection_id,
            ints.int_geom,
            nodes.node_id,
            node_geom
        FROM congestion.%I  nodes
        CROSS JOIN LATERAL ( 
                SELECT node_id, nodes.geom AS node_geom, intersection_id, ints.geom AS int_geom
                FROM gis_core.%I ints 
                ORDER BY ST_TRANSFORM(nodes.geom, 2952) <-> ST_TRANSFORM(ints.geom, 2952)
                LIMIT 1
            ) AS ints
		 CROSS JOIN LATERAL ( 
                SELECT intersection_id, px, signals.geom AS px_geom
                FROM gis.traffic_signal signals   
                ORDER BY ST_TRANSFORM(signals.geom, 2952) <-> ST_TRANSFORM(int_geom, 2952)
                LIMIT 1
            ) AS signals
  	    WHERE nodes.node_id = %s',
		   nodes_table, centreline_name, nodes)
;
END;

$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION congestion.add_new_int_px(nodes numeric, new_ver_id TEXT, centreline_ver text)
IS 'Function to return specific node''s intersection and px match ';

ALTER FUNCTION congestion.add_new_int_px(nodes numeric, new_ver_id TEXT, centreline_ver text) owner to congestion_admins;
		   