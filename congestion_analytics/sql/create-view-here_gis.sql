CREATE OR REPLACE VIEW  here_analysis.here_gis AS 
SELECT b.link_dir, CASE WHEN RIGHT(link_dir,1) = 'T' THEN ST_Transform(A.geom,98012) ELSE ST_Reverse(ST_Transform(A.geom,98012)) END AS geom
FROM here_gis.streets_18_3 a
INNER JOIN here_analysis.analysis_links b ON LEFT(b.link_dir,-1) = a.link_id::text;