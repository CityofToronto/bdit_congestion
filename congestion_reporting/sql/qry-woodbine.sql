DROP TABLE IF EXISTS temp_data;

CREATE TEMPORARY TABLE temp_data (
	group_id integer,
	group_order integer,
	dt daterange,
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
	
INSERT INTO 	temp_data(group_id, group_order, dt, corridor_id, corridor_name, street, direction, intersection_start, intersection_end, length_km, hh, tt_average, tt_median, tt_lower, tt_upper)
SELECT 		C.group_id, C.group_order, A.dt, C.corridor_id, C.corridor_name, C.street, C.direction, C.intersection_start, C.intersection_end, C.length_km, A.hh, SUM(tt_avg) AS tt_average, SUM(tt_med) AS tt_median, SUM(tt_15) as tt_lower, SUM(tt_85) as tt_upper
FROM 		here_analysis.corridor_link_agg A
INNER JOIN	here_analysis.corridor_links USING (link_dir, corridor_id)
INNER JOIN	here_analysis.corridors C USING (corridor_id)

WHERE 		C.group_id IN (29,30)
		-- AND A.dt = '[2015-09-01,2015-10-30)'
		AND A.day_type = 496
GROUP BY 	A.dt, C.corridor_id, C.corridor_name, C.street, C.direction, C.intersection_start, C.intersection_end, C.length_km, A.hh
HAVING 		C.num_links = COUNT(*)
ORDER BY 	C.group_id, C.group_order, A.dt, A.hh;

SELECT	group_id,
	dt,
	corridor_id,
	concat(street,' ',direction) AS main_street,
	intersection_start AS from_cross_street, 
	intersection_end AS to_cross_street, 
	length_km AS distance_km,
	round(AVG(CASE WHEN hh IN (7,7.5,8,8.5,9,9.5) THEN tt_average ELSE NULL END),1) AS am_avg_tt,
	round(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (7,7.5,8,8.5,9,9.5) THEN tt_median ELSE NULL END))::numeric,1) AS am_med_tt,
	round(AVG(CASE WHEN hh IN (11,11.5,12,12.5) THEN tt_average ELSE NULL END),1) AS off_avg_tt,
	round(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (11,11.5,12,12.5) THEN tt_median ELSE NULL END))::numeric,1) AS off_med_tt,
	round(AVG(CASE WHEN hh IN (16,16.5,17,17.5,18,18.5) THEN tt_average ELSE NULL END),1) AS pm_avg_tt,
	round(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN hh IN (16,16.5,17,17.5,18,18.5) THEN tt_median ELSE NULL END))::numeric,1) AS pm_med_tt,
	SUM(CASE WHEN hh IN (7,7.5,8,8.5,9,9.5) THEN 1 ELSE 0 END) AS am_count,
	SUM(CASE WHEN hh IN (11,11.5,12,12.5) THEN 1 ELSE 0 END) AS off_count,
	SUM(CASE WHEN hh IN (16,16.5,17,17.5,18,18.5) THEN 1 ELSE 0 END) AS pm_count

FROM	temp_data
GROUP BY group_id, dt, corridor_id, group_order, concat(street,' ',direction), intersection_start, intersection_end, length_km
ORDER BY group_id, group_order, dt