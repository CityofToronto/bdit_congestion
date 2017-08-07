DROP TABLE IF EXISTS temp_data;

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

WHERE 		C.group_id IN (1,2,11,12) 
		AND A.dt = '[2017-04-30,2017-05-18)'
GROUP BY 	C.corridor_id, C.corridor_name, C.street, C.direction, C.intersection_start, C.intersection_end, C.length_km, A.hh
HAVING 		C.num_links = COUNT(*)
ORDER BY 	C.group_id, C.group_order, A.hh;

SELECT	concat(street,' ',direction) AS main_street,
	intersection_start AS from_cross_street, 
	intersection_end AS to_cross_street, 
	length_km AS distance_km,
	round(AVG(CASE WHEN hh IN (7,7.5,8,8.5,9,9.5) THEN tt_average ELSE NULL END),1) AS am_tt,
	round(length_km/AVG(CASE WHEN hh IN (7,7.5,8,8.5,9,9.5) THEN tt_average ELSE NULL END)*3600.0,1) AS am_spd,
	round(AVG(CASE WHEN hh IN (12,12.5,13,13.5,14,14.5) THEN tt_average ELSE NULL END),1) AS off_tt,
	round(length_km/AVG(CASE WHEN hh IN (12,12.5,13,13.5,14,14.5) THEN tt_average ELSE NULL END)*3600.0,1) AS off_spd,
	round(AVG(CASE WHEN hh IN (15.75,16.25,16.75,17.25,17.75,18.25) THEN tt_average ELSE NULL END),1) AS pm_tt,
	round(length_km/AVG(CASE WHEN hh IN (15.75,16.25,16.75,17.25,17.75,18.25) THEN tt_average ELSE NULL END)*3600.0,1) AS pm_spd
FROM	temp_data
GROUP BY group_id, group_order, concat(street,' ',direction), intersection_start, intersection_end, length_km
ORDER BY group_id, group_order