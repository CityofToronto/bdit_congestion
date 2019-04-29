DROP MATERIALIZED VIEW here_analysis.dec_5pm_ttr_map;
CREATE MATERIALIZED VIEW here_analysis.dec_5pm_ttr_map AS
SELECT B.link_dir, B.ttr, ST_OffsetCurve(ST_Transform(A.geom, 98012),CASE WHEN RIGHT(B.link_dir,1) = 'F' THEN -20 ELSE 20 END)
FROM here_gis.streets_18_3 A
INNER JOIN here_analysis.dec_5pm_ttr B ON A.link_id = LEFT(B.link_dir,-1)::numeric;
 