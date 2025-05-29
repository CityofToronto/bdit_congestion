-- Creating network_segments from network_links_21_1 
SELECT
    segment_id,
    start_vid,
    end_vid,
    ST_linemerge(ST_union(geom)) AS geom,
    round(ST_length(ST_transform(ST_linemerge(ST_union(geom)), 2952))::numeric, 2) AS total_length

FROM congestion.network_links_21_1
GROUP BY segment_id, start_vid, end_vid
ORDER BY segment_id, start_vid, end_vid;

