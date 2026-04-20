-- SELECT congestion.update_congestion_highway(	'temp_congestion_segments_25_1','temp_congestion_centreline_25_1','20260301');
-- Make sure that centreline lookup table has all centreline matched rows needed

CREATE OR REPLACE FUNCTION congestion.update_congestion_highway(
    table_to_update text,
    centreline_lookup text,
    centreline_version text
)
RETURNS void AS $$
DECLARE
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
         AND %I.highway::boolean IS DISTINCT FROM a.highway_tf',
        table_to_update,
        centreline_lookup,
		centreline_version_table,
        table_to_update,
        table_to_update
    );

    EXECUTE format(
        'UPDATE congestion.%I
         SET highway = false
         WHERE highway is null',
        table_to_update
    );

END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION congestion.update_congestion_highway(text, text, text)
    OWNER TO congestion_admins;
