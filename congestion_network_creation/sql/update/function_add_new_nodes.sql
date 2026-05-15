'''
    select congestion.add_new_nodes(30356897,'24_4');
    '''

CREATE OR REPLACE FUNCTION congestion.add_new_nodes(
    node_id numeric,
    ver_id text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE

    new_routing text := 'routing_nodes_'|| ver_id;
    temp_table text := 'temp_congestion_nodes_'|| ver_id;
BEGIN

EXECUTE format(
'INSERT INTO congestion.%I
SELECT distinct nodes.node_id::bigint, nodes.geom, %L AS ver_id
FROM here.%I nodes
WHERE node_id = %s',
temp_table, ver_id, new_routing, node_id
);

END;
$$;

ALTER FUNCTION congestion.add_new_nodes(numeric, text)
    OWNER TO congestion_admins;

COMMENT ON FUNCTION congestion.add_new_nodes(numeric, text)
IS 'Functions to add a single new node to desired temp table.';
