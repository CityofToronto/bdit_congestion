-- FUNCTION: congestion.rebuild_temp_nodes_lookup(text, text)

-- DROP FUNCTION IF EXISTS congestion.rebuild_temp_nodes_lookup(text, text);

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
    intersection_ver text := 'intersection_' || centreline_ver;
BEGIN

    -- Create the new lookup table from the previous version with existing ints
    EXECUTE format('
        CREATE TABLE congestion.%s AS
        SELECT  DISTINCT prev.node_id, 
				prev.intersection_id, 
				prev.px, 
				ic.intersection_desc,
				ic.highest_order_feature,
				prev.node_geom,
				a.geom AS int_geom, 
				%L AS ver_id	
        FROM congestion.%s prev
        INNER JOIN gis_core.centreline_intersection_point_latest a USING (intersection_id)
		INNER JOIN congestion.%s nodes USING (node_id)
		LEFT JOIN gis_core.intersection_classification ic on ic.intersection_id = prev.intersection_id
    ', new_table, new_ver, prev_table, nodes_table);

    -- Insert new nodes lookup from previous steps
    EXECUTE format('
        INSERT INTO congestion.%s
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
        LEFT JOIN gis_core.intersection_classification ic 
            USING (intersection_id)
    ', new_table, new_ver, source_table);

END;
$BODY$;

ALTER FUNCTION congestion.rebuild_temp_nodes_lookup(text, text)
    OWNER TO congestion_admins;