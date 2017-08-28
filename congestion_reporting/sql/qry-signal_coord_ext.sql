DROP TABLE IF EXISTS temp_data;

CREATE TEMPORARY TABLE temp_data (
	group_id integer,
	group_order integer,
	corridor_id integer,
	corridor_name character varying(100),
	street character varying(50),
	direction character varying(10),
	intersection_start character varying(50),
	intersection_end character varying(50),
	length_km numeric,
	hh numeric,
	tt_average numeric,
	tt_median numeric,
	tt_lower numeric,
	tt_upper numeric
	);
	
INSERT INTO 	temp_data(group_id, group_order, corridor_id, corridor_name, street, direction, intersection_start, intersection_end, length_km, hh, tt_average, tt_median, tt_lower, tt_upper)
SELECT 		C.group_id, C.group_order, C.corridor_id, C.corridor_name, C.street, C.direction, C.intersection_start, C.intersection_end, C.length_km, A.hh, SUM(tt_avg) AS tt_average, SUM(tt_med) AS tt_median, SUM(tt_15) as tt_lower, SUM(tt_85) as tt_upper
FROM 		here_analysis.corridor_link_agg A
INNER JOIN	here_analysis.corridor_links USING (link_dir, corridor_id)
INNER JOIN	here_analysis.corridors C USING (corridor_id)

WHERE 		C.group_id IN (13,14,21,22,23,24) 
		-- AND A.dt = '[2017-05-01,2017-05-18)'
GROUP BY 	C.corridor_id, C.corridor_name, C.street, C.direction, C.intersection_start, C.intersection_end, C.length_km, A.hh
HAVING 		C.num_links = COUNT(*)
ORDER BY 	C.group_id, C.group_order, A.hh;

SELECT	group_id,
	corridor_id,
	concat(street,' ',direction) AS main_street,
	intersection_start AS from_cross_street, 
	intersection_end AS to_cross_street, 
	length_km AS distance_km,
	round(AVG(CASE WHEN hh IN (7,7.5,8,8.5,9,9.5) THEN tt_average ELSE NULL END),1) AS am_avg_tt,
	round(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (7,7.5,8,8.5,9,9.5) THEN tt_median ELSE NULL END))::numeric,1) AS am_med_tt,
	round(PERCENTILE_CONT(0) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (7,7.5,8,8.5,9,9.5) THEN tt_lower ELSE NULL END))::numeric,1) AS am_min_tt,
	round(PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (7,7.5,8,8.5,9,9.5) THEN tt_upper ELSE NULL END))::numeric,1) AS am_max_tt,
	round(length_km/AVG(CASE WHEN hh IN (7,7.5,8,8.5,9,9.5) THEN tt_average ELSE NULL END)*3600.0,1) AS am_avg_spd,
	round(length_km/(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (7,7.5,8,8.5,9,9.5) THEN tt_median ELSE NULL END))::numeric)*3600.0,1) AS am_med_spd,
	round(length_km/(PERCENTILE_CONT(0) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (7,7.5,8,8.5,9,9.5) THEN tt_lower ELSE NULL END))::numeric)*3600.0,1) AS am_max_spd,
	round(length_km/(PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (7,7.5,8,8.5,9,9.5) THEN tt_upper ELSE NULL END))::numeric)*3600.0,1) AS am_min_spd,
	round(AVG(CASE WHEN hh IN (12,12.5,13,13.5,14,14.5) THEN tt_average ELSE NULL END),1) AS off_avg_tt,
	round(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (12,12.5,13,13.5,14,14.5) THEN tt_median ELSE NULL END))::numeric,1) AS off_med_tt,
	round(PERCENTILE_CONT(0) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (12,12.5,13,13.5,14,14.5) THEN tt_lower ELSE NULL END))::numeric,1) AS off_min_tt,
	round(PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (12,12.5,13,13.5,14,14.5) THEN tt_upper ELSE NULL END))::numeric,1) AS off_max_tt,
	round(length_km/AVG(CASE WHEN hh IN (12,12.5,13,13.5,14,14.5) THEN tt_average ELSE NULL END)*3600.0,1) AS off_avg_spd,
	round(length_km/(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (12,12.5,13,13.5,14,14.5) THEN tt_median ELSE NULL END))::numeric)*3600.0,1) AS off_med_spd,
	round(length_km/(PERCENTILE_CONT(0) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (12,12.5,13,13.5,14,14.5) THEN tt_lower ELSE NULL END))::numeric)*3600.0,1) AS off_max_spd,
	round(length_km/(PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (12,12.5,13,13.5,14,14.5) THEN tt_upper ELSE NULL END))::numeric)*3600.0,1) AS off_min_spd,
	round(AVG(CASE WHEN hh IN (15.5,16,16.5,17,17.5,18) THEN tt_average ELSE NULL END),1) AS pm_avg_tt,
	round(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (15.5,16,16.5,17,17.5,18) THEN tt_median ELSE NULL END))::numeric,1) AS pm_med_tt,
	round(PERCENTILE_CONT(0) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (15.5,16,16.5,17,17.5,18) THEN tt_lower ELSE NULL END))::numeric,1) AS pm_min_tt,
	round(PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (15.5,16,16.5,17,17.5,18) THEN tt_upper ELSE NULL END))::numeric,1) AS pm_max_tt,
	round(length_km/AVG(CASE WHEN hh IN (15.5,16,16.5,17,17.5,18) THEN tt_average ELSE NULL END)*3600.0,1) AS pm_avg_spd,
	round(length_km/(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (15.5,16,16.5,17,17.5,18) THEN tt_median ELSE NULL END))::numeric)*3600.0,1) AS pm_med_spd,
	round(length_km/(PERCENTILE_CONT(0) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (15.5,16,16.5,17,17.5,18) THEN tt_lower ELSE NULL END))::numeric)*3600.0,1) AS pm_max_spd,
	round(length_km/(PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (15.5,16,16.5,17,17.5,18) THEN tt_upper ELSE NULL END))::numeric)*3600.0,1) AS pm_min_spd

FROM	temp_data
GROUP BY group_id, corridor_id, group_order, concat(street,' ',direction), intersection_start, intersection_end, length_km
ORDER BY group_id, group_order