CREATE MATERIALIZED VIEW  here_analysis.here_gis AS 
SELECT CONCAT(a.link_id,b) AS link_dir, CASE WHEN b = 'T' THEN ST_Transform(A.geom,98012) ELSE ST_Reverse(ST_Transform(A.geom,98012)) END AS geom
FROM here_gis.streets_18_3 a
CROSS JOIN unnest(ARRAY['T','F']) b