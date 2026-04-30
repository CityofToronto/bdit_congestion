'''
Sample Usage: 
SELECT * FROM congestion.add_new_segments(
    30434395, 
    30443790,
    'routing_streets_24_4',
    'temp_network_links_24_4',
    'temp_network_segments_24_4',
    NULL,                         -- _exclude_links
    '25_1',                      -- _ver_id
    TRUE                         -- _dry_run
);
'''

-- FUNCTION: congestion.add_new_segments(integer, integer, text, text, text, boolean)

-- DROP FUNCTION IF EXISTS congestion.add_new_segments(integer, integer, text, text, text, boolean);

CREATE OR REPLACE FUNCTION congestion.add_new_segments(
	_start_vid integer,
	_end_vid integer,
	_routing_streets_table text,
	_temp_network_links_table text,
	_temp_network_segments_table text,
    _exclude_links text,
    _ver_id text,
	_dry_run boolean DEFAULT false)
    RETURNS TABLE(segment_id bigint, start_vid integer, end_vid integer, link_dir text, geom geometry, cost double precision, id bigint, seq integer, node bigint, edge bigint, agg_cost double precision) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    max_segment_id BIGINT;
    routing_sql TEXT;
BEGIN
    -- Get the max segment_id from temp_network_links table
    EXECUTE format('SELECT COALESCE(MAX(segment_id), 0) FROM congestion.%I', _temp_network_links_table)
    INTO max_segment_id;
 
    IF _exclude_links IS NOT NULL THEN    
        routing_sql := format('SELECT 
                id, 
                source::int, 
                target::int, 
                ST_LENGTH(ST_TRANSFORM(geom, 2952)) AS cost 
            FROM here.%I WHERE link_dir != %L', _routing_streets_table, _exclude_links);
    ELSE
        routing_sql := format('SELECT 
            id, 
            source::int, 
            target::int, 
            ST_LENGTH(ST_TRANSFORM(geom, 2952)) AS cost 
        FROM here.%I', _routing_streets_table);
    END IF;
    -- Return results from the routing query
    RETURN QUERY
    EXECUTE format('
        WITH intersections AS (
            SELECT 
                ROW_NUMBER() OVER() + %L::int AS segment_id,
                %L::int AS start_vid,
                %L::int AS end_vid
        ),
        results AS (
            SELECT 
                i.segment_id,
                i.start_vid,
                i.end_vid,
                routing_grid.link_dir,
                routing_grid.geom,
                ST_LENGTH(ST_TRANSFORM(routing_grid.geom, 2952)) AS cost,
                routing_grid.id,
                djk.seq,
                djk.node,
                djk.edge,
                djk.agg_cost
            FROM intersections i
            , LATERAL  pgr_dijkstra(%L, i.start_vid, i.end_vid) djk
            INNER JOIN here.%I routing_grid ON routing_grid.id = djk.edge
        )
        SELECT * FROM results
    ', max_segment_id, _start_vid, _end_vid, routing_sql, _routing_streets_table);
    
    -- If not a dry run, execute the inserts
    IF NOT _dry_run THEN
        -- Insert into temp_network_links table
        EXECUTE format('
            WITH intersections AS (
                SELECT 
                    ROW_NUMBER() OVER() + %L::int AS segment_id,
                    %L::int AS start_vid,
                    %L::int AS end_vid
            ),
            results AS (
                SELECT 
                    i.segment_id,
                    i.start_vid,
                    i.end_vid,
                    routing_grid.link_dir,
                    routing_grid.geom,
                    ST_LENGTH(ST_TRANSFORM(routing_grid.geom, 2952)) AS cost,
                    routing_grid.id,
                    djk.seq,
                    djk.node,
                    djk.edge,
                    djk.agg_cost
                FROM intersections i
                , LATERAL pgr_dijkstra(%L, i.start_vid, i.end_vid) djk
                INNER JOIN here.%I routing_grid ON routing_grid.id = djk.edge
            ),
            insert_links AS (
                INSERT INTO congestion.%I (segment_id, start_vid, end_vid, link_dir, geom, length, ver_id)
                SELECT 
                    segment_id,
                    start_vid,
                    end_vid,
                    link_dir,
                    geom,
                    cost,
                    %L
                FROM results
            )
            INSERT INTO congestion.%I (segment_id, start_vid, end_vid, total_length, dir,highway, geom, ver_id)
            SELECT 
                segment_id,
                start_vid,
                end_vid,
                SUM(cost), 
                gis.direction_from_line(ST_union(geom)) AS dir,
                null as highway,
                ST_linemerge(ST_union(geom)),
                %L
            FROM results
            GROUP BY segment_id, start_vid, end_vid
        ', max_segment_id, _start_vid, _end_vid, routing_sql, _routing_streets_table, _temp_network_links_table, _ver_id, _temp_network_segments_table, _ver_id);

        RAISE NOTICE 'segment_id % inserted successfully from % to % using tables: %, %, %', 
            max_segment_id, _start_vid, _end_vid, _routing_streets_table, _temp_network_links_table, _temp_network_segments_table;
    ELSE
        RAISE NOTICE 'DRY RUN! No data inserted. Check to see if the result is correct before inserting a new segment.';
    END IF;
END;
$BODY$;

COMMENT ON FUNCTION congestion.add_new_segments(integer, integer, text, text, text, text, text, boolean)

IS 'Add new segments to both link and segments table.';

ALTER FUNCTION congestion.add_new_segments(integer, integer, text, text, text, text, text, boolean)
    OWNER TO congestion_admins;