TRUNCATE here_analysis.corridor_link_agg;

INSERT INTO here_analysis.corridor_link_agg (corridor_id, link_dir, dt, day_type, hh, tt_avg, tt_med, obs)

WITH VAR AS (
	SELECT 	5 as corridor_id,
		1 as dt1, -- monday
		1 as dt2, -- tuesday
		1 as dt3, -- wednesday
		1 as dt4, -- thursday
		1 as dt5, -- friday
		0 as dt6, -- saturday
		0 as dt7, -- sunday
		0 as dt8, -- holidays
		0 as dt9, -- holiday extensions
		'2017-05-01'::date as date_start,
		'2017-05-18'::date as date_end)
		
SELECT 		VAR.corridor_id, 
		B.link_dir, 
		daterange(VAR.date_start,VAR.date_end) AS dt,
		(CONCAT(VAR.dt1,VAR.dt2,VAR.dt3,VAR.dt4,VAR.dt5,VAR.dt6,VAR.dt7,VAR.dt8,VAR.dt9)::bit(9))::int AS day_type,
		EXTRACT(HOUR from A.tx) + (EXTRACT(minute FROM A.tx)/30)::int*0.5 as hh,
		ST_Length(ST_Transform(D.geom,32190)) * AVG(1.0/A.pct_50) * 3.6 AS tt_avg,
		ST_Length(ST_Transform(D.geom,32190)) / (PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY A.pct_50)) * 3.6 AS tt_med,
		COUNT(A.pct_50) AS obs
FROM 		VAR
CROSS JOIN 	here.ta A
INNER JOIN 	here_analysis.corridor_links B USING (link_dir, corridor_id)
INNER JOIN 	here_analysis.corridors C USING (corridor_id)
INNER JOIN 	here_gis.streets_16_1 D ON LEFT(B.link_dir,-1)::numeric = D.link_id
LEFT JOIN	ref.holiday E ON e.dt = A.tx::date

WHERE 		C.corridor_id = VAR.corridor_id 
		AND A.tx::date >= VAR.date_start 
		AND A.tx::date <= VAR.date_end
		AND EXTRACT(dow FROM A.tx) IN (1*dt1,2*dt2,3*dt3,4*dt4,5*dt5,6*dt6,7*dt7)
		AND CASE WHEN dt8 = 0 THEN E.dt IS NULL END
		
GROUP BY 	VAR.corridor_id, 
		B.link_dir,
		daterange(VAR.date_start,VAR.date_end),
		(CONCAT(VAR.dt1,VAR.dt2,VAR.dt3,VAR.dt4,VAR.dt5,VAR.dt6,VAR.dt7,VAR.dt8,VAR.dt9)::bit(9))::int,
		EXTRACT(HOUR from A.tx)  + (EXTRACT(minute FROM A.tx)/30)::int*0.5,
		ST_Length(ST_Transform(D.geom,32190))