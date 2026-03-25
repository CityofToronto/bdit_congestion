CREATE OR REPLACE FUNCTION congestion.rebuild_temp_segments(new_ver text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    temp_link_table text := format('temp_network_links_%s', new_ver);
	temp_table text := format('temp_network_segments_%s', new_ver);
BEGIN

EXECUTE format(
'CREATE TABLE congestion.%I AS
SELECT segment_id,
       start_vid,
       end_vid,
       ST_linemerge(ST_union(geom)) AS geom,
       %L AS ver_id,
	   round(ST_length(ST_transform(ST_linemerge(ST_union(geom)), 2952))::numeric, 2) AS total_length
FROM congestion.%I
group by segment_id,start_vid, end_vid; ',
temp_table, new_ver, temp_link_table
);
END;
$$;