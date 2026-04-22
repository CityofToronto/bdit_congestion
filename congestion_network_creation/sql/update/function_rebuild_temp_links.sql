CREATE OR REPLACE FUNCTION congestion.rebuild_temp_links( old_ver text, new_ver text )
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    old_links text := 'congestion_links_'|| old_ver;
    new_routing text := 'routing_streets_'|| new_ver;
    temp_table text := 'temp_congestion_links_'|| new_ver;
BEGIN

----------------------------------------------------
-- Create links with new geom
----------------------------------------------------
EXECUTE format(
'CREATE TABLE congestion.%I AS
SELECT segment_id,
       start_vid,
       end_vid,
       r.link_dir,
       r.length,
       r.geom,
       %L AS ver_id
FROM congestion.%I
INNER JOIN here.%I r USING (link_dir);',
temp_table, new_ver, old_links, new_routing
);

EXECUTE format(
'ALTER TABLE congestion.%I OWNER TO congestion_admins;',
temp_table
);

----------------------------------------------------
-- Delete incomplete segments
----------------------------------------------------
EXECUTE format(
'
WITH changed_links AS (
    SELECT link_dir FROM congestion.%I
    EXCEPT
    SELECT link_dir FROM here.%I
),
changed_seg AS (
    SELECT DISTINCT segment_id, start_vid, end_vid
    FROM congestion.%I
    INNER JOIN changed_links USING (link_dir)
)
DELETE FROM congestion.%I t
USING changed_seg
WHERE changed_seg.segment_id = t.segment_id;
',
old_links, new_routing, old_links, temp_table
);

----------------------------------------------------
-- Insert re-routed ones
----------------------------------------------------
EXECUTE format(
'
WITH changed_links AS (
    SELECT link_dir FROM congestion.%I
    EXCEPT
    SELECT link_dir FROM here.%I
),
changed_seg AS (
    SELECT DISTINCT segment_id, start_vid, end_vid
    FROM congestion.%I
    INNER JOIN changed_links USING (link_dir)
)
INSERT INTO congestion.%I
SELECT segment_id,
       start_vid,
       end_vid,
       link_dir,
       round(st_length(st_transform(routing.geom,2952))::numeric,2) AS length,
	   routing.geom,
       %L AS ver_id
FROM changed_seg
CROSS JOIN LATERAL pgr_dijkstra(
    %L,
    start_vid,
    end_vid
)
INNER JOIN here.%I routing
ON id = edge;
',
old_links,new_routing,old_links,temp_table,new_ver,
format(
    'SELECT id, source::int, target::int,
            st_length(st_transform(geom,2952)) AS cost
     FROM here.%I', new_routing
),
new_routing
);

END;
$$;


COMMENT ON FUNCTION congestion.rebuild_temp_links( old_ver text, new_ver text )
IS 'Functions to rebuild temp congestion links table.';

ALTER FUNCTION congestion.rebuild_temp_links( old_ver text, new_ver text )
    OWNER TO congestion_admins;