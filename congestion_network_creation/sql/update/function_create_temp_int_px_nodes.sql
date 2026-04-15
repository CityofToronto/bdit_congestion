CREATE OR REPLACE FUNCTION congestion.create_temp_int_px_nodes(old_ver_id TEXT, new_ver_id TEXT, centreline_ver text)
RETURNS TABLE (
    px INT,
    px_geom GEOMETRY,
    intersection_id INT,
    int_geom GEOMETRY,
    node_id BIGINT,
    node_geom GEOMETRY
) AS $$
DECLARE
    table_name text := 'temp_int_px_nodes_' || new_ver_id;
    routing_nodes_table text := 'routing_nodes_' || new_ver_id;
    centreline_name text := 'intersection_'|| new_ver_id; 
	lookup text := 'congestion_nodes_lookup_' || old_ver_id;
BEGIN

    EXECUTE format(
        'CREATE TABLE congestion.%I AS
        WITH new_signals AS (
            SELECT traffic_signal.* 
            FROM gis.traffic_signal
            LEFT JOIN (
                SELECT px::INT FROM congestion.excluded_signals
                UNION 
                SELECT px::INT FROM congestion.%I
            ) old_signal ON traffic_signal.px::INT = old_signal.px::INT
            WHERE removed_date IS NULL 
              AND temp_signal IS NULL 
              AND old_signal.px IS NULL
        ),
        int_joins AS (
            SELECT DISTINCT ints.px, ints.px_geom, intersection_id, int_geom
            FROM new_signals signals 
            CROSS JOIN LATERAL ( 
                SELECT px, signals.geom AS px_geom, intersection_id, ints.geom AS int_geom
                FROM gis_core.%I ints 
                ORDER BY ST_TRANSFORM(signals.geom, 2952) <-> ST_TRANSFORM(ints.geom, 2952)
                LIMIT 1
            ) AS ints
        )
        SELECT DISTINCT 
            int_joins.px,
            int_joins.px_geom,
            int_joins.intersection_id,
            int_joins.int_geom,
            nodes.node_id,
            nodes.geom AS node_geom
        FROM int_joins
        LEFT JOIN here.%I nodes ON 
            ST_DWITHIN(ST_TRANSFORM(int_joins.int_geom, 2952), ST_TRANSFORM(nodes.geom, 2952), 25
            )',
        table_name,
		lookup,
		centreline_name,
        routing_nodes_table
    );
    
END;
$$ LANGUAGE plpgsql;

alter function congestion.create_temp_int_px_nodes(old_ver_id TEXT, new_ver_id TEXT, centreline_ver text) owner to congestion_admins;
