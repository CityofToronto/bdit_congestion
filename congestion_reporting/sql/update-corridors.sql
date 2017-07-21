UPDATE here_analysis.corridors

SELECT corridor_id, SUM(ST_Length(ST_Transform(B.geom,32190)))/1000.0 AS length
FROM here_analysis.corridor_links A
INNER JOIN here_gis.streets_16_1 B ON LEFT(a.link_dir,-1)::numeric = B.link_id
GROUP BY corridor_id
ORDER BY corridor_id
