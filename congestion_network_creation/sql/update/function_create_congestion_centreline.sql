CREATE OR REPLACE FUNCTION congestion.create_congestion_centreline(
    new_ver TEXT,   -- e.g. '25_1'
    old_ver TEXT,   -- e.g. '24_4'
    centreline_ver TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    output_table   TEXT := 'temp_congestion_centreline_'  || new_ver;
    congestion_centreline_old        TEXT := 'congestion_centreline_'        || old_ver;
    new_seg   TEXT := 'temp_network_segments_'        || new_ver;
    old_seg  TEXT := 'congestion_segments_'          || old_ver;
    new_nodes_lookup   TEXT := 'temp_congestion_nodes_lookup_' || new_ver;
    centreline_table TEXT := 'routing_centreline_directional_higher_rc_'|| centreline_ver;
    routing_restrictions TEXT:= 'centreline_routing_restrictions_higher_rc_'||centreline_ver;
BEGIN

    EXECUTE format('
        CREATE TABLE %1$s AS
        WITH outdated_segments AS (
            SELECT DISTINCT segment_id, from_int, to_int
            FROM (
                SELECT segment_id, from_int, to_int, unnest(centreline_ids) AS centreline_id, geom
                FROM congestion.%2$s
            ) a
            LEFT JOIN gis_core.%6$s USING (centreline_id)
            WHERE centreline_latest.centreline_id IS NULL
        ),
        new_segments AS (
            SELECT segment_id, start_vid, end_vid
            FROM congestion.%3$s
            EXCEPT
            SELECT segment_id, start_vid, end_vid
            FROM congestion.%4$s
            ORDER BY segment_id
        ),
        new_segments_w_ints AS (
            SELECT
                segment_id,
                s.intersection_id AS from_int,
                e.intersection_id AS to_int
            FROM new_segments
            LEFT JOIN congestion.%5$s s ON start_vid  = s.node_id
            LEFT JOIN congestion.%5$s e ON end_vid    = e.node_id
        ),
        need_update AS (
            SELECT * FROM outdated_segments
            UNION
            SELECT * FROM new_segments_w_ints
        ),
        results AS (
            SELECT
                t.from_int, t.to_int,
                edge, cost, agg_cost, seq, segment_id, node, path_seq
            FROM need_update t
            CROSS JOIN LATERAL pgr_trsp(
                $q$
                    SELECT
                        id,
                        source::int,
                        target::int,
                        cost_length::int AS cost
                    FROM gis_core.%6$s
                $q$,
                $q$
                    SELECT path, cost
                    FROM gis_core.%7$s
                $q$,
                t.from_int, t.to_int, true
            ) AS route
        )
        SELECT
            d.segment_id,
            d.from_int,
            d.to_int,
            array_agg(r.centreline_id ORDER BY path_seq) AS centreline_ids,
            ST_LineMerge(ST_Union(r.geom))               AS geom
        FROM results d
        JOIN gis_core.routing_centreline_directional_higher_rc_20260301 r ON d.edge = r.id
        GROUP BY d.segment_id, d.from_int, d.to_int
    ',
        output_table,   -- %1$s
        congestion_centreline_old,        -- %2$s
        new_seg,   -- %3$s
        old_seg,  -- %4$s
        new_nodes_lookup,    -- %5$s 
        centreline_table, -- $6
        routing_restrictions --7
    );

END;
$$;