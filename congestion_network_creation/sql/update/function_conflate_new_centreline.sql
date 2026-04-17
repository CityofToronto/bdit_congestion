
CREATE OR REPLACE FUNCTION congestion.conflate_unchanged_centreline(
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
    new_segment TEXT := 'temp_congestion_segments_'|| new_ver;

BEGIN
        EXECUTE format('
			INSERT INTO congestion.%I
			WITH temp as (
			SELECT 
				distinct segment_id, from_int, to_int
			FROM (SELECT segment_id, from_int, to_int, unnest(centreline_uids) as centreline_uid, geom 
			        FROM congestion.%I) a
			LEFT JOIN gis_core.%I centreline USING (centreline_uid)
			WHERE centreline.centreline_uid IS NULL)
			
			, prep AS (
			SELECT con.segment_id, con.from_int, con.to_int, unnest(centreline_uids) AS centreline_uid, dir
			FROM congestion.%I con
			INNER JOIN congestion.%I USING (segment_id)
			LEFT JOIN temp USING (segment_id)
			WHERE temp.segment_id is null)
			
			SELECT  segment_id, from_int, to_int, 
					array_agg(centreline_id) AS centreline_ids,
					array_agg(centreline_uid) AS uids,
					ST_LineMerge(ST_Union(geom)) AS geom
			from prep
			inner join gis_core.%I using (centreline_uid)
			GROUP BY segment_id, from_int, to_int, prep.dir;
', create_table, old_centreline_lookup, routing_centreline_Table, old_centreline_lookup, new_segment, routing_centreline_Table);
 
END;
$BODY$;

ALTER FUNCTION congestion.conflate_unchanged_centreline(text, text, text)
    OWNER TO congestion_admins;