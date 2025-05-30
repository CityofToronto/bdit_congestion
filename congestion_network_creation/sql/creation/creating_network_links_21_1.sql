CREATE TABLE congestion.network_links_21_1 AS

WITH temp AS (
    SELECT
        segment_id,
        start_vid,
        end_vid,
        unnest(link_set) AS link_uid
    FROM congestion.network_routing_results
)

SELECT
    segment_id,
    link_dir,
    geom,
    round(ST_length(st_transform(geom, 2952))::numeric, 2) AS length
FROM temp
INNER JOIN here.routing_streets_21_1 ON id = link_uid
ORDER BY segment_id;

COMMENT ON TABLE congestion.network_links_21_1 IS
'A lookup table for the congestion network, contains segment_id and link_dir (21_1 map version) lookup.';