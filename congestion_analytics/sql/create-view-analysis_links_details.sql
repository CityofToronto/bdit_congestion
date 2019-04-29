DROP MATERIALIZED VIEW here_analysis.analysis_links_details;
CREATE MATERIALIZED VIEW here_analysis.analysis_links_details AS
SELECT B.link_dir, ST_Length(ST_Transform(A.geom, 98012)) AS length_m
FROM here_gis.streets_18_3 A
INNER JOIN here_analysis.analysis_links B ON A.link_id = LEFT(B.link_dir,-1)::numeric
ORDER BY B.link_dir;