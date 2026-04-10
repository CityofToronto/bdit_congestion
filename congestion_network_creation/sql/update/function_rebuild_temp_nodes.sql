CREATE OR REPLACE FUNCTION congestion.rebuild_temp_nodes(
    old_ver text,
    new_ver text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    old_nodes text := 'congestion_nodes_%s'|| old_ver;
    new_routing text := 'routing_nodes_%s'|| new_ver;
    temp_table text := 'temp_congestion_nodes_%s'|| new_ver;
BEGIN

EXECUTE format(
'CREATE TABLE congestion.%I AS
SELECT distinct nodes.node_id::bigint, nodes.geom, %L AS ver_id
FROM here.%I nodes
INNER JOIN congestion.%I r USING (node_id)',
temp_table, new_ver, new_routing, old_nodes
);

EXECUTE format(
'ALTER TABLE congestion.%I OWNER TO congestion_admins;',
temp_table
);

END;
$$;

ALTER FUNCTION congestion.rebuild_temp_nodes(text, text)
    OWNER TO congestion_admins;