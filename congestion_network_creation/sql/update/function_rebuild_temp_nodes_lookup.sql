---
---SELECT congestion.rebuild_temp_nodes_lookup(
---    '24_4' , -- older version
---	'25_1', -- new version
---    '20260301') --centreline version
---
CREATE OR REPLACE FUNCTION congestion.rebuild_temp_nodes_lookup(
	old_ver text,
	new_ver text,
    centreline_ver text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    new_table text := 'temp_congestion_nodes_lookup_' || new_ver;
    prev_table text := 'congestion_nodes_lookup_' || old_ver;
	nodes_table text := 'temp_congestion_nodes_' || new_ver;
    source_table text := 'temp_int_px_nodes_' || new_ver;
    intersection_ver text := 'centreline_intersection_point_' || centreline_ver;
    centreline_table text := 'centreline_' || centreline_ver;
BEGIN

    -- Create the new lookup table from the previous version with existing ints
    EXECUTE format('
        CREATE TABLE congestion.%I AS
     	WITH highest_order_table AS 
    	(select intersection_id, intersection_desc, array_length(array_agg(centreline_id), 1) AS degree,
    		(array_agg(feature_code_desc ORDER BY feature_code))[1] AS highest_order_feature, geom
    	FROM gis_core.%I
    	left join (select from_intersection_id AS intersection_id, centreline_id, feature_code, feature_code_desc 
    				from gis_core.%I
    				union 
    				select to_intersection_id, centreline_id, feature_code, feature_code_desc 
    				from gis_core.%I) AS a USING (intersection_id)
    	group by intersection_id, intersection_desc, geom)
            SELECT  DISTINCT 
                    prev.node_id, 
    				prev.intersection_id, 
    				prev.px, 
    				ic.intersection_desc,
    				ic.highest_order_feature,
    				nodes.geom AS geom,
    				COALESCE(ic.geom, prev.int_geom) AS int_geom, -- if intersection does not have a geom we use the old one
    				%L AS ver_id	
            FROM congestion.%I prev
            INNER JOIN congestion.%I nodes USING (node_id)
    		LEFT JOIN highest_order_table ic on ic.intersection_id = prev.intersection_id
    ', new_table, intersection_ver, centreline_table, centreline_table,new_ver, prev_table, nodes_table);

    -- Insert new nodes lookup from previous steps
    EXECUTE format('
        INSERT INTO congestion.%s
        WITH highest_order_table AS 
    	(select intersection_id, intersection_desc, array_length(array_agg(centreline_id), 1) AS degree,
    		(array_agg(feature_code_desc ORDER BY feature_code))[1] AS highest_order_feature, geom
    	FROM gis_core.%I
    	left join (select from_intersection_id AS intersection_id, centreline_id, feature_code, feature_code_desc 
    				from gis_core.%I
    				union 
    				select to_intersection_id, centreline_id, feature_code, feature_code_desc 
    				from gis_core.%I) AS a USING (intersection_id)
    	group by intersection_id, intersection_desc, geom)
        SELECT
            n.node_id,
            n.intersection_id,
            n.px,
            ic.intersection_desc,
            ic.highest_order_feature,
            n.node_geom,
            n.int_geom,
            %L AS ver_id
        FROM congestion.%s n
        LEFT JOIN gis_core.%I a USING (intersection_id)
        LEFT JOIN highest_order_table ic on ic.intersection_id = a.intersection_id
    ', new_table, intersection_ver, centreline_table, centreline_table, new_ver, source_table, intersection_ver);

END;
$BODY$;

ALTER FUNCTION congestion.rebuild_temp_nodes_lookup(text, text, text)
    OWNER TO congestion_admins;

COMMENT ON FUNCTION congestion.rebuild_temp_nodes_lookup(text, text, text)
IS 'Functions to rebuild temp nodes lookup table.';

COMMENT ON FUNCTION congestion.rebuild_temp_nodes_lookup(text, text, text)
IS 'Functions to rebuild temp nodes lookup table.';
