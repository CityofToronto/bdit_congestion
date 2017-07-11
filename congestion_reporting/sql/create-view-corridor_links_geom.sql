DROP MATERIALIZED VIEW here_analysis.corridor_links_geom;

CREATE MATERIALIZED VIEW here_analysis.corridor_links_geom AS
SELECT CASE WHEN RIGHT(B.link_dir,1) = 'F' THEN A.geom ELSE ST_Reverse(A.geom) END as geom, B.*
FROM here_gis.streets_16_1 A
INNER JOIN here_analysis.corridor_links B ON LEFT(B.link_dir,-1)::numeric = A.link_id;