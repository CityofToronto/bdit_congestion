-- SELECT update_congestion_highway('congestion.temp', 'congestion.temp_congestion_centreline_25_5');
-- Make sure that centreline lookup table has all centreline matched rows needed

CREATE OR REPLACE FUNCTION congestion.update_congestion_highway(
    table_to_update text,
    centreline_lookup text,
    centreline_version text
)
RETURNS integer AS $$
DECLARE
    v_rows_affected integer;
	centreline_version_table text:= 'centreline_'||centreline_version;
BEGIN
    EXECUTE format(
        'UPDATE congestion.%I
         SET highway = a.highway_tf
         FROM (
            SELECT 
                segment_id, 
                min(feature_code) = 201100 AS highway_tf
            FROM (
                SELECT 
                    segment_id, 
                    unnest(centreline_ids) AS centreline_id
                FROM congestion.%I
            ) unnested
            INNER JOIN gis_core.%I USING (centreline_id)
            GROUP BY segment_id
            HAVING min(feature_code) = 201100
         ) a
         WHERE a.segment_id = %I.segment_id
         AND %I.highway IS DISTINCT FROM a.highway_tf',
        table_to_update,
		centreline_version_table,
        centreline_lookup,
        table_to_update,
        table_to_update
    );
    -- returns the number of rows that changed
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RETURN v_rows_affected;
END;
$$ LANGUAGE plpgsql;
