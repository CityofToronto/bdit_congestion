SELECT 	CONCAT(C.street,' ',C.direction) AS main_street, 
	C.intersection_start AS from_cross_street, 
	C.intersection_end AS to_cross_street,
	A.hh,
	SUM(tt_avg) AS average_tt, COUNT(B.*) AS num_links, SUM(ST_Length(ST_Transform(D.geom,32190)))/1000.0 AS distance_km
FROM here_analysis.corridor_link_agg A
INNER JOIN here_analysis.corridor_links B USING (corridor_id, link_dir)
INNER JOIN here_analysis.corridors C USING (corridor_id)
INNER JOIN here_gis.streets_16_1 D ON LEFT(B.link_dir,-1)::numeric = D.link_id
GROUP BY CONCAT(C.street,' ',C.direction), C.intersection_start, C.intersection_end, A.hh
ORDER BY CONCAT(C.street,' ',C.direction), C.intersection_start, C.intersection_end, A.hh

