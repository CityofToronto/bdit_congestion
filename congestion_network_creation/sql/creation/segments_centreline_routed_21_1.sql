-- Routes centreline using int_id <-> node_id look up table
-- results in centreline that makes up each segment
--SELECT 5740
--Query returned successfully in 30 min 36 secs.

CREATE TABLE congestion.segments_centreline_routed_21_1 AS

WITH segment_info AS (
    SELECT
        segment_id,
        start_vid,
        end_vid,
        fnode.int_id AS start_int,
        tnode.int_id AS end_int
    FROM congestion.network_segments
    LEFT JOIN congestion.network_int_px_21_1 AS fnode ON network_segments.start_vid = fnode.node_id
    LEFT JOIN congestion.network_int_px_21_1 AS tnode ON network_segments.end_vid = tnode.node_id
    WHERE
        fnode.int_id != tnode.int_id
        AND fnode.int_id IS NOT NULL
        AND tnode.int_id IS NOT NULL
),

result AS (
    SELECT *
    FROM segment_info
    CROSS JOIN LATERAL pgr_dijkstra(
        'SELECT id, source::int, target::int, cost
				 	   				 FROM gis.centreline_routing_20220705_undirected',
        start_int,
        end_int,
        FALSE
    )
)

SELECT
    segment_id,
    start_vid,
    end_vid,
    start_int,
    end_int,
    array_agg(geo_id),
    ST_linemerge(ST_union(a.geom)) AS geom
FROM result
INNER JOIN gis.centreline_20220705 AS a ON geo_id = edge
GROUP BY segment_id, start_vid, end_vid, start_int, end_int
