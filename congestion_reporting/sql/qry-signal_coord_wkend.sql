﻿DROP TABLE IF EXISTS temp_data;

CREATE TEMPORARY TABLE temp_data (
	group_id integer,
	group_order integer,
	corridor_name character varying(100),
	street character varying(50),
	direction character varying(10),
	intersection_start character varying(50),
	intersection_end character varying(50),
	length_km numeric,
	hh numeric,
	tt_average numeric);
	
INSERT INTO 	temp_data(group_id, group_order, corridor_name, street, direction, intersection_start, intersection_end, length_km, hh, tt_average)
SELECT 		C.group_id, C.group_order, C.corridor_name, C.street, C.direction, C.intersection_start, C.intersection_end, C.length_km, A.hh, SUM(tt_avg) AS tt_average
FROM 		here_analysis.corridor_link_agg A
INNER JOIN	here_analysis.corridor_links USING (link_dir, corridor_id)
INNER JOIN	here_analysis.corridors C USING (corridor_id)

WHERE 		A.day_type = 12
		-- C.group_id IN (1,2,11,12) 
		AND A.dt = '[2017-07-08,2017-07-30)'
GROUP BY 	C.corridor_id, C.corridor_name, C.street, C.direction, C.intersection_start, C.intersection_end, C.length_km, A.hh
HAVING 		C.num_links = COUNT(*)
ORDER BY 	C.group_id, C.group_order, A.hh;

SELECT	concat(street,' ',direction) AS main_street,
	intersection_start AS from_cross_street, 
	intersection_end AS to_cross_street, 
	length_km AS distance_km,
	round(AVG(CASE WHEN hh IN (12,13,14,15,16) THEN tt_average ELSE NULL END),1) AS wkend_tt,
	round(length_km/AVG(CASE WHEN hh IN (12,13,14,15,16) THEN tt_average ELSE NULL END)*3600.0,1) AS wkend_spd
FROM	temp_data
GROUP BY group_id, group_order, concat(street,' ',direction), intersection_start, intersection_end, length_km
ORDER BY group_id, group_order