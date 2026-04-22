CREATE OR REPLACE FUNCTION congestion.rebuild_temp_segments(new_ver text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    temp_link_table text := 'temp_congestion_links_'|| new_ver;
	temp_table text := 'temp_congestion_segments_'|| new_ver;
BEGIN

EXECUTE format(
'CREATE TABLE congestion.%I AS
SELECT  segment_id,
        start_vid,
        end_vid,
        round(ST_length(ST_transform(ST_linemerge(ST_union(geom)), 2952))::numeric, 2)::double precision AS total_length,
        gis.direction_from_line(ST_linemerge(ST_union(geom)))::text AS dir,
        NULL::boolean AS highway,
        ST_linemerge(ST_union(geom)) AS geom,
        %L AS ver_id
FROM congestion.%I
group by segment_id,start_vid, end_vid; ',
temp_table, new_ver, temp_link_table
);
END;
$$;

ALTER FUNCTION congestion.rebuild_temp_segments(text)
    OWNER TO congestion_admins;

COMMENT ON FUNCTION congestion.rebuild_temp_segments(text)
IS 'Functions to rebuild temp segment table.';
