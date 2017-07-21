TRUNCATE	here_analysis.corridor_links_15min;

INSERT INTO 	here_analysis.corridor_links_15min

SELECT 		C.corridor_id, 
		B.link_dir, 
		date_trunc('hour', A.tx) + INTERVAL '30 min' * floor(date_part('minute', A.tx) / 30.0) as datetime_bin,
		B.seq,
		1/AVG(1.0/A.pct_50) AS spd_avg,
		B.distance_km * AVG(1.0/A.pct_50) * 3600 AS tt_avg,
		COUNT(A.pct_50) AS obs,
		FALSE as excluded,
		FALSE as estimated
		
FROM 		here.ta A
INNER JOIN 	here_analysis.corridor_links B USING (link_dir)
INNER JOIN 	here_analysis.corridors C USING (corridor_id)
LEFT JOIN	ref.holiday E ON e.dt = A.tx::date

WHERE		A.tx::date >= '2016-01-01'
		AND A.tx::date < '2017-01-01'
		AND EXTRACT(dow FROM A.tx) IN (1,2,3,4,5)
		AND E.dt IS NULL
		AND ((corridor_id > 161 AND corridor_id <= 175) OR (corridor_id > 330 AND corridor_id <= 342))
		
GROUP BY 	C.corridor_id, 
		B.link_dir,
		date_trunc('hour', A.tx) + INTERVAL '30 min' * floor(date_part('minute', A.tx) / 30.0),
		B.seq,
		B.distance_km
ORDER BY 	C.corridor_id,
		date_trunc('hour', A.tx) + INTERVAL '30 min' * floor(date_part('minute', A.tx) / 30.0),
		B.seq;

