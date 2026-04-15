CREATE OR REPLACE FUNCTION congestion.seperate_segments_w_nodes(
	ver_id text,
	dry_run boolean DEFAULT true)
    RETURNS TABLE(new_segment_id bigint, start_vid bigint, end_vid bigint, link_dir text, geom geometry) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE

AS $BODY$
DECLARE
    nodes_table TEXT;
    links_table TEXT;
    segments_table TEXT;
    routing_table TEXT;
    node_id_val BIGINT;
BEGIN

    nodes_table := format('temp_int_px_nodes_%s', ver_id);
    links_table := format('temp_congestion_links_%s', ver_id);
    segments_table := format('temp_congestion_segments_%s', ver_id);
    routing_table := format('routing_streets_%s', ver_id);

    -- loop through each node one by one
    FOR node_id_val IN 
        EXECUTE format('SELECT node_id FROM congestion.%I', nodes_table)
    LOOP
        RETURN QUERY
        EXECUTE format('
            WITH find_affected_segments AS (
                SELECT DISTINCT segment_id, start_vid, end_vid
                FROM congestion.%I
                INNER JOIN here.%I USING (link_dir)
                WHERE source = %L OR target = %L
            ),
            construct_new_sets AS (
                SELECT ROW_NUMBER() OVER() + (SELECT MAX(segment_id) FROM congestion.%I) AS new_segment_id, 
                       new_sets.*
                FROM (
                    SELECT start_vid, %L::bigint AS end_vid
                    FROM find_affected_segments
                    UNION
                    SELECT %L::bigint AS start_vid, end_vid
                    FROM find_affected_segments
                ) new_sets
            ),
            results AS (
                SELECT 
                    construct_new_sets.new_segment_id::bigint,
                    construct_new_sets.start_vid::bigint,
                    construct_new_sets.end_vid::bigint,
                    routing_grid.link_dir,
                    routing_grid.geom
                FROM construct_new_sets
                , LATERAL pgr_dijkstra(
                    ''SELECT id, source::int, target::int, ST_LENGTH(ST_TRANSFORM(geom, 2952)) AS cost
                       FROM here.%I''::text,
                    construct_new_sets.start_vid::int,
                    construct_new_sets.end_vid::int
                ) results
                INNER JOIN here.%I routing_grid ON routing_grid.id = results.edge
            )
            SELECT * FROM results
        ', links_table, routing_table, node_id_val, node_id_val, links_table, 
           node_id_val, node_id_val, routing_table, routing_table);
        
        -- If not a dry run, do the insert/delete for this node
        IF NOT dry_run THEN
            EXECUTE format('
                WITH find_affected_segments AS (
                    SELECT DISTINCT segment_id, start_vid, end_vid
                    FROM congestion.%I
                    INNER JOIN here.%I USING (link_dir)
                    WHERE source = %L OR target = %L
                ),
                construct_new_sets AS (
                    SELECT ROW_NUMBER() OVER() + (SELECT MAX(segment_id) FROM congestion.%I) AS new_segment_id, 
                           new_sets.*
                    FROM (
                        SELECT start_vid, %L::bigint AS end_vid
                        FROM find_affected_segments
                        UNION
                        SELECT %L::bigint AS start_vid, end_vid
                        FROM find_affected_segments
                    ) new_sets
                ),
                results AS (
                    SELECT 
                        results.*,
                        construct_new_sets.new_segment_id::bigint,
                        routing_grid.link_dir::text,
                        construct_new_sets.start_vid::bigint,
                        construct_new_sets.end_vid::bigint,
                        routing_grid.id,
                        routing_grid.geom
                    FROM construct_new_sets
                    , LATERAL pgr_dijkstra(
                        ''SELECT id, source::int, target::int, ST_LENGTH(ST_TRANSFORM(geom, 2952)) AS cost
                           FROM here.%I''::text,
                        construct_new_sets.start_vid::int,
                        construct_new_sets.end_vid::int
                    ) results
                    INNER JOIN here.%I routing_grid ON routing_grid.id = results.edge
                ),
                insert_links AS (
                    INSERT INTO congestion.%I (segment_id, start_vid, end_vid, link_dir, geom, length)
                    SELECT new_segment_id, start_vid, end_vid, link_dir, geom, cost
                    FROM results
                ),
                insert_segments AS (
                    INSERT INTO congestion.%I (segment_id, start_vid, end_vid, dir, geom, total_length)
                    SELECT new_segment_id, start_vid, end_vid, gis.direction_from_line(ST_LINEMERGE(ST_UNION(geom))) AS dir, ST_LINEMERGE(ST_UNION(geom)), SUM(cost)
                    FROM results
                    GROUP BY new_segment_id, start_vid, end_vid
                ),
                delete_old_links AS (
                    DELETE FROM congestion.%I
                    WHERE segment_id IN (SELECT segment_id FROM find_affected_segments)
                )
                DELETE FROM congestion.%I
                WHERE segment_id IN (SELECT segment_id FROM find_affected_segments)
            ', links_table, routing_table, node_id_val, node_id_val, links_table,
               node_id_val, node_id_val, routing_table, routing_table, links_table,
               segments_table, links_table, segments_table);
               
            RAISE NOTICE 'Processed node %', node_id_val;
        END IF;
    END LOOP;

    IF dry_run THEN
        RAISE NOTICE 'DRY RUN! No data modified. Check results above before executing with dry_run = FALSE.';
    ELSE
        RAISE NOTICE 'Successfully added all nodes and reconstructed segments for version: %', ver_id;
    END IF;
END;
$BODY$;