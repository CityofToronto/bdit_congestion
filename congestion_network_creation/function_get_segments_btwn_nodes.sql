CREATE OR REPLACE FUNCTION congestion.get_segments_btwn_nodes(
	start_node integer,
	end_node integer,
	OUT start_node integer,
	OUT end_node integer,
	OUT segment_list int[],
	OUT length numeric,
	OUT geom geometry)
    RETURNS record
    LANGUAGE 'sql'
    COST 100
    STABLE STRICT PARALLEL UNSAFE
AS $BODY$
WITH results as (
	SELECT * 
	FROM pgr_dijkstra('SELECT segment_id as id, source::int, target::int,cost from congestion.network_segment_routing', start_node, end_node)
)

SELECT start_node, end_node, array_agg(segment_id::int), round(sum(ST_length(ST_transform(geom, 2952)))::numeric,2) as length, ST_union(ST_linemerge(geom)) as geom
from   results
inner join congestion.network_segments on edge=segment_id
$BODY$;

COMMENT ON FUNCTION congestion.get_segments_btwn_nodes(integer, integer)
    IS 'Function created for routing segments in the network_segments using here nodes.  ';

ALTER FUNCTION congestion.get_segments_btwn_nodes(integer, integer)
    OWNER TO congestion_admins;
    
    
GRANT EXECUTE ON FUNCTION congestion.get_segments_btwn_nodes(integer, integer) TO bdit_humans;
    