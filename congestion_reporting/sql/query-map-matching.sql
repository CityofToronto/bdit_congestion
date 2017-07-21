SELECT	corridor_load_id,
	CONCAT(A.link_id,
		CASE WHEN ST_LineLocatePoint(st_linemerge(b.geom),ST_StartPoint(ST_Transform(a.geom,32190))) <
			ST_LineLocatePoint(st_linemerge(b.geom),ST_EndPoint(ST_Transform(a.geom,32190))) THEN 'F'
		ELSE 'T'
		END) AS link_dir,
	ROW_NUMBER() OVER (ORDER BY line_dist(ST_Transform(a.geom,32190), b.geom)) AS seq,
	ST_Length(ST_Transform(a.geom,32190))/1000.0 AS distance_km,
	dist_frac(ST_Transform(a.geom,32190),b.geom) as x,
	angle_diff(ST_Transform(a.geom,32190), b.geom) as y
FROM here_gis.streets_16_1 a
INNER JOIN here_analysis.corridor_load b ON ST_DWithin(ST_Centroid(ST_Transform(a.geom,32190)), b.geom, 15)
INNER JOIN here_gis.streets_att_16_1 c USING (link_id)
WHERE 	corridor_load_id = 37
ORDER BY line_dist(ST_Transform(a.geom,32190), b.geom) asc;
