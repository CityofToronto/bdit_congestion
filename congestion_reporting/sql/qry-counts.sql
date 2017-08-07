SELECT corridor_id, link_dir, seq, ST_Length(ST_Transform(C.geom,32190)) as length_m, COUNT(*) 
FROM here_analysis.corridor_links A
LEFT JOIN here_analysis.corridor_link_agg B USING (corridor_id,link_dir)
LEFT JOIN here_gis.streets_16_1 C ON LEFT(B.link_dir,-1)::numeric = C.link_id
WHERE hh IN (7,8,9)
GROUP BY corridor_id, link_dir, seq, ST_Length(ST_Transform(C.geom,32190))
ORDER BY corridor_id, seq, link_dir