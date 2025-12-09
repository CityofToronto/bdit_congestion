CREATE OR REPLACE FUNCTION congestion.route_contracted_path(
    _node_start      bigint,
    _node_end        bigint,
    _contracted_ver  bigint[], 
    OUT _node_start_out bigint,
    OUT _node_end_out   bigint,
    OUT links           bigint[],
    OUT geom            geometry
)
RETURNS record
LANGUAGE plpgsql
AS $func$
DECLARE
    edges_sql text;
BEGIN
    edges_sql := format(
        'SELECT
             id,
             source::int,
             target::int,
             cost_length::int AS cost
         FROM congestion.routing_centreline_directional 
         WHERE source = ANY(%L::int[]) AND target = ANY(%L::int[])',
        _contracted_ver,
        _contracted_ver
    );

    WITH results AS (
        SELECT *
        FROM pgr_dijkstra(
            edges_sql,
            _node_start,
            _node_end,
            true
        )
    ),
    agg AS (
        SELECT
            _node_start AS start_node,
            _node_end   AS end_node,
            array_agg(r.centreline_id ORDER BY path_seq) AS link_arr,
            ST_LineMerge(ST_Union(r.geom)) AS geom_out
        FROM results d
        JOIN  congestion.routing_centreline_directional r ON d.edge = r.id
    )
    SELECT
        start_node,
        end_node,
        link_arr,
        geom_out
    INTO
        _node_start_out,
        _node_end_out,
        links,
        geom
    FROM agg;

    RETURN;
END;
$func$;


ALTER FUNCTION congestion.route_contracted_path(bigint, bigint, bigint[])
    OWNER TO congestion_admins;