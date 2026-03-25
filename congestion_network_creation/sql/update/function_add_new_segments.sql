'''
Sample Usage: 
SELECT * FROM congestion.add_new_segments(
    30434395, 
    30443790,
    'routing_streets_24_4',
    'temp_network_links_24_4',
    'temp_network_segments_24_4',
    FALSE
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
 
    routing_sql := format('SELECT 
        id, 
        source::int, 
        target::int, 
        ST_LENGTH(ST_TRANSFORM(geom, 2952)) AS cost 
    FROM here.%I', _routing_streets_table);
    
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
                INSERT INTO congestion.%I (segment_id, start_vid, end_vid, link_dir, geom, length)
                SELECT 
                    segment_id,
                    start_vid,
                    end_vid,
                    link_dir,
                    geom,
                    cost
                FROM results
            )
            INSERT INTO congestion.%I (segment_id, start_vid, end_vid, geom, total_length)
            SELECT 
                segment_id,
                start_vid,
                end_vid,
                ST_linemerge(ST_union(geom)),
                SUM(cost)
            FROM results
            GROUP BY segment_id, start_vid, end_vid
        ', max_segment_id, _start_vid, _end_vid, routing_sql, _routing_streets_table, _temp_network_links_table, _temp_network_segments_table);

        RAISE NOTICE 'segment_id % inserted successfully from % to % using tables: %, %, %', 
            max_segment_id, _start_vid, _end_vid, _routing_streets_table, _temp_network_links_table, _temp_network_segments_table;
    ELSE
        RAISE NOTICE 'DRY RUN! No data inserted. Check to see if the result is correct before inserting a new segment.';
    END IF;
END;
$BODY$;

ALTER FUNCTION congestion.add_new_segments(integer, integer, text, text, text, boolean)
    OWNER TO congestion_admins;