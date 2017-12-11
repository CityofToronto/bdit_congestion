-- TRUNCATE here_analysis.corridor_link_agg;

INSERT INTO here_analysis.corridor_link_test (corridor_id, link_dir, dt, month_bin, day_type, hh, tt_avg, tt_med, obs, tt_65, tt_75, tt_85, tt_95, tt_35, tt_25, tt_15, tt_05)

WITH VAR AS (	SELECT 
		1 as dt1, -- monday
		1 as dt2, -- tuesday
		1 as dt3, -- wednesday
		1 as dt4, -- thursday
		1 as dt5, -- friday
		0 as dt6, -- saturday
		0 as dt7, -- sunday
		0 as dt8, -- holidays
		0 as dt9, -- holiday extensions
		'2016-06-07'::date as date_start,
		'2016-06-23'::date as date_end)
		
SELECT 		C.corridor_id, 
		B.link_dir, 
		daterange(VAR.date_start,VAR.date_end) AS dt,
		date_trunc('month', A.tx)::date as month_bin,
		(CONCAT(VAR.dt1,VAR.dt2,VAR.dt3,VAR.dt4,VAR.dt5,VAR.dt6,VAR.dt7,VAR.dt8,VAR.dt9)::bit(9))::int AS day_type,
		EXTRACT(HOUR from A.tx) + floor(EXTRACT(minute FROM A.tx)/30)::int*0.5 as hh,
		B.distance_km * AVG(1.0/A.pct_50) * 3600.0 AS tt_avg,
		B.distance_km / (PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY A.pct_50)) * 3600.0 AS tt_med,
		COUNT(A.pct_50) AS obs,
		B.distance_km / (PERCENTILE_CONT(0.35) WITHIN GROUP(ORDER BY A.pct_50)) * 3600.0 AS tt_65,
		B.distance_km / (PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY A.pct_50)) * 3600.0 AS tt_75,
		B.distance_km / (PERCENTILE_CONT(0.15) WITHIN GROUP(ORDER BY A.pct_50)) * 3600.0 AS tt_85,
		B.distance_km / (PERCENTILE_CONT(0.05) WITHIN GROUP(ORDER BY A.pct_50)) * 3600.0 AS tt_95,
		B.distance_km / (PERCENTILE_CONT(0.65) WITHIN GROUP(ORDER BY A.pct_50)) * 3600.0 AS tt_35,
		B.distance_km / (PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY A.pct_50)) * 3600.0 AS tt_15,
		B.distance_km / (PERCENTILE_CONT(0.85) WITHIN GROUP(ORDER BY A.pct_50)) * 3600.0 AS tt_25,
		B.distance_km / (PERCENTILE_CONT(0.95) WITHIN GROUP(ORDER BY A.pct_50)) * 3600.0 AS tt_05
FROM 		VAR
CROSS JOIN 	here.ta A
INNER JOIN 	here_analysis.corridor_links B USING (link_dir)
INNER JOIN 	here_analysis.corridors C USING (corridor_id)
LEFT JOIN	ref.holiday E ON e.dt = A.tx::date

WHERE		A.tx::date >= VAR.date_start 
		AND A.tx::date <= VAR.date_end
		AND EXTRACT(dow FROM A.tx) IN (1*dt1,2*dt2,3*dt3,4*dt4,5*dt5,6*dt6,7*dt7)
		AND CASE WHEN dt8 = 0 THEN E.dt IS NULL END
		AND C.group_id IN (95,96,97,98,99,100)
		AND A.tx::date IN ('2016-06-07','2016-06-08','2016-06-09','2016-06-14','2016-06-15','2016-06-16','2016-06-21','2016-06-22','2016-06-23')
		
GROUP BY 	C.corridor_id, 
		B.link_dir,
		daterange(VAR.date_start,VAR.date_end),
		date_trunc('month', A.tx),
		(CONCAT(VAR.dt1,VAR.dt2,VAR.dt3,VAR.dt4,VAR.dt5,VAR.dt6,VAR.dt7,VAR.dt8,VAR.dt9)::bit(9))::int,
		EXTRACT(HOUR from A.tx) + floor(EXTRACT(minute FROM A.tx)/30)::int*0.5,
		B.distance_km;