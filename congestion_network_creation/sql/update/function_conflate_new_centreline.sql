-- FUNCTION: congestion.conflate_new_centreline(text, text, text)

-- DROP FUNCTION IF EXISTS congestion.conflate_new_centreline(text, text, text);

CREATE OR REPLACE FUNCTION congestion.conflate_new_centreline(
	old_ver text,
	new_ver text,
	centreline_ver text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    centreline_table TEXT := 'centreline_'|| centreline_ver;
    create_table TEXT := 'temp_congestion_centreline_'|| new_ver;
    old_centreline_lookup TEXT := 'congestion_centreline_'|| old_ver;
    routing_centreline_Table  TEXT := 'routing_centreline_directional_higher_rc_'|| centreline_ver;
    nodes_table TEXT := 'temp_congestion_nodes_lookup_'|| new_ver;
    old_segment TEXT := 'congestion_segments_'|| old_ver;
    new_segment TEXT := 'temp_congestion_segments_'|| new_ver;
    routing_restriction  TEXT := 'centreline_routing_restrictions_higher_rc_'|| centreline_ver;

BEGIN
        EXECUTE format('
            CREATE TABLE congestion.%I AS
            WITH outdated_segments AS (
            SELECT distinct segment_id, from_int, to_int
            FROM (SELECT old.segment_id, old.from_int, old.to_int, unnest(old.centreline_ids) as centreline_id
                    FROM congestion.%I old
            		INNER JOIN congestion.%I USING (segment_id)) a
            LEFT JOIN gis_core.%I centreline USING (centreline_id)
            WHERE centreline.centreline_id IS NULL)
            
            ,  new_segments AS (
            SELECT segment_id, start_vid, end_vid
            FROM congestion.%I
            EXCEPT
            SELECT  segment_id, start_vid, end_vid 
            FROM congestion.%I
            ORDER BY segment_id)
            
            , new_segments_w_ints AS 
            (SELECT segment_id, s.intersection_id as from_int, e.intersection_id as to_int
            FROM new_segments
            left join congestion.%I s on start_vid = node_id
            left join congestion.%I e on end_vid = e.node_id)
            
            , need_update AS 
            (SELECT * FROM outdated_segments
            union 
            SELECT * FROM new_segments_w_ints)
            
            , results AS (
                SELECT t.from_int, t.to_int, edge, cost, agg_cost, seq, segment_id, node, path_seq
                FROM need_update t
                CROSS JOIN LATERAL pgr_trsp(
                    $$
                    SELECT
                        id,
                        source::int,
                        target::int,
                        cost_length::int AS cost
                    FROM gis_core.%I
                    $$,
            		$$SELECT path, cost FROM gis_core.%I $$,
                    t.from_int, t.to_int,true
                ) AS route)
            	
            SELECT d.segment_id, 
            		d.from_int, 
            		d.to_int, 
            		array_agg(r.centreline_id ORDER BY path_seq) AS centreline_ids,
            		array_agg(r.centreline_uid ORDER BY path_seq) AS centreline_uids,
            		ST_LineMerge(ST_Union(r.geom)) AS geom
            from results d
            JOIN gis_core.%I r ON d.edge = r.id
            GROUP BY d.segment_id, d.from_int, d.to_int;
        ', create_table, old_centreline_lookup, new_segment, centreline_table,new_segment,  old_segment, nodes_table, nodes_table, 
    routing_centreline_Table, routing_restriction, routing_centreline_Table);
 
END;
$BODY$;

COMMENT ON FUNCTION congestion.conflate_new_centreline(text, text, text)
IS 'Function to insert changed centreline with new geometry to temp_congestion_centreline.';

ALTER FUNCTION congestion.conflate_new_centreline(text, text, text)
    OWNER TO congestion_admins;