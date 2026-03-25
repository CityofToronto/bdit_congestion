CREATE OR REPLACE FUNCTION congestion.create_temp_int_px_nodes(ver_id TEXT)
RETURNS TABLE (
    px INT,
    px_geom GEOMETRY,
    intersection_id INT,
    int_geom GEOMETRY,
    node_id BIGINT,
    node_geom GEOMETRY
) AS $$
DECLARE
    table_name TEXT;
    routing_nodes_table TEXT;
BEGIN
    table_name := 'temp_int_px_nodes_' || ver_id;
    routing_nodes_table := 'here.routing_nodes_' || ver_id;

    EXECUTE format(
        'CREATE TABLE congestion.%I AS
        WITH new_signals AS (
            SELECT traffic_signal.* 
            FROM gis.traffic_signal
            LEFT JOIN (
                SELECT px::INT FROM congestion.excluded_signals
                UNION 
                SELECT px::INT FROM congestion.int_px_nodes_latest
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
                FROM gis_core.intersection_latest ints 
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
        LEFT JOIN %I nodes ON 
            ST_DWITHIN(ST_TRANSFORM(int_joins.int_geom, 2952), ST_TRANSFORM(nodes.geom, 2952), 25
            )',
        table_name,
        routing_nodes_table
    );
    
END;
$$ LANGUAGE plpgsql;
