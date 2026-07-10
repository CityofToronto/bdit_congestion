-- FUNCTION: congestion.get_congestion_segments_btwn_ints(integer, integer, text)

-- DROP FUNCTION IF EXISTS congestion.get_congestion_segments_btwn_ints(integer, integer, text);

CREATE OR REPLACE FUNCTION congestion.get_congestion_segments_btwn_ints(
	start_vid integer,
	end_vid integer,
	ver_id text DEFAULT NULL::text,
	OUT start_node integer,
	OUT end_node integer,
	OUT segment_list integer[],
	OUT length numeric,
	OUT geom geometry)
    RETURNS record
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    congestion_table text := format('congestion_segments_%s', ver_id);
    lookup_table text := format('congestion_nodes_lookup_%s', ver_id);
BEGIN  
   EXECUTE format($f$
        with all_pairs as (
select * FROM 
(select node_id as start_node FROM congestion.%4$I 
where intersection_id = %2$L::int) start_node
cross join (select node_id as end_node FROM congestion.%4$I
where intersection_id = %3$L::int) end_node)

, results AS (
      SELECT
   		 ap.start_node,
  		  ap.end_node,
  		  d.*
			FROM all_pairs ap
			CROSS JOIN LATERAL
			    pgr_dijkstra(
			        'SELECT
			            segment_id AS id,
			            start_vid::integer AS source,
			            end_vid::integer AS target,
			            total_length::float8 AS cost
			         FROM congestion.%1$I',
			        ap.start_node,
			        ap.end_node
  		  ) AS d
        )
        SELECT
            start_node,
            end_node,
            array_agg(segment_id::int ORDER BY path_seq),
            round(sum(ST_length(ST_transform(geom, 2952)))::numeric, 2) AS length,
            ST_union(ST_linemerge(geom)) AS geom
        FROM results
        INNER JOIN congestion.%1$I ON edge = segment_id
		GROUP BY start_node, end_node
		order by length
		limit 1 
    $f$, congestion_table, start_vid, end_vid, lookup_table)
	INTO start_node, end_node, segment_list, length, geom;
	RETURN;

END;
$BODY$;

ALTER FUNCTION congestion.get_congestion_segments_btwn_ints(integer, integer, text)
    OWNER TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.get_congestion_segments_btwn_ints(integer, integer, text) TO PUBLIC;

GRANT EXECUTE ON FUNCTION congestion.get_congestion_segments_btwn_ints(integer, integer, text) TO bdit_humans;

GRANT EXECUTE ON FUNCTION congestion.get_congestion_segments_btwn_ints(integer, integer, text) TO congestion_admins;

COMMENT ON FUNCTION congestion.get_congestion_segments_btwn_ints(integer, integer, text)
    IS 'Functions to input start and end node_id and return the congestion segments in between those two input.';
