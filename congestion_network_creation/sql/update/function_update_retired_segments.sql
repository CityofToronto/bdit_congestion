CREATE OR REPLACE FUNCTION congestion.update_retired_segments(old_ver text, new_ver text)
RETURNS void
LANGUAGE plpgsql
AS $$ --routing_centreline_directional_higher_rc
DECLARE

	old_segments text := 'congestion_segments_'|| old_ver;
	old_links text := 'congestion_links_'|| old_ver;
	new_segments text := 'congestion_segments_'|| new_ver;
	new_links text := 'congestion_links_'|| new_ver;
	
BEGIN
    -- Create partition table
    EXECUTE format(
		'INSERT INTO congestion.congestion_retired_segments
		WITH retired_segments AS (
		SELECT segment_id FROM congestion.%I
		except
		SELECT segment_id FROM congestion.%I)
		
		
		SELECT old.segment_id AS old_segment_id, array_agg(distinct new.segment_id) AS new_segment_ids, %L AS old_ver, %L AS new_ver
		FROM congestion.%I old
		join retired_segments USING (segment_id)
		join congestion.%I new USING (link_dir)
		group by old.segment_id;',
        old_segments, new_segments, old_ver, new_ver, old_links, new_links
    );

END;
$$;

ALTER FUNCTION congestion.update_retired_segments(text, text)
    OWNER TO congestion_admins;

COMMENT ON FUNCTION  congestion.update_retired_segments(text, text)
IS 'Functions to insert new retired segments';