-- corridor_links: corridor_link_id (auto), corridor_id, link_dir, seq, distance_km
-- corridors: corridor_id (auto), corridor_name (input), length_km (input), street (input), direction (input), group_id (NULL), group_order (NULL)


SELECT 	b.corridor_load_id,
	a.link_id, 
	line_dist(ST_Transform(a.geom,32190), b.geom) AS r,
	
	ST_LineLocatePoint(st_linemerge(b.geom),ST_StartPoint(ST_Transform(a.geom,32190))) as st,
	ST_LineLocatePoint(st_linemerge(b.geom),ST_EndPoint(ST_Transform(a.geom,32190))) as en
	FROM here_gis.streets_16_1 a
INNER JOIN here_analysis.corridor_load b ON ST_DWithin(ST_Transform(a.geom,32190), b.geom, 5)
WHERE 	b.corridor_load_id >= 1
	AND angle_diff(ST_Transform(a.geom,32190), b.geom) < 10 
	AND ST_DWithin(ST_Centroid(ST_Transform(a.geom,32190)), b.geom,5)
ORDER BY b.corridor_load_id asc, line_dist(ST_Transform(a.geom,32190), b.geom) asc