UPDATE here_analysis.corridor_links A
SET distance_km = ST_Length(ST_Transform(B.geom,32190))/1000.0
FROM here_gis.streets_16_1 B
WHERE A.corridor_id >= 837 AND B.link_id = SUBSTR(A.link_dir, 1,LENGTH(A.link_dir)-1)::numeric;

UPDATE here_analysis.corridor_links A
SET tot_distance_km = (SELECT SUM(B.distance_km) FROM here_analysis.corridor_links B WHERE B.corridor_id = A.corridor_id AND B.seq <= A.seq)
WHERE A.corridor_id >= 837;