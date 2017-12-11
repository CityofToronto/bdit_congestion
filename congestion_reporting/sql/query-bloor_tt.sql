SELECT 		C.group_id,
		C.group_order,
		C.corridor_id,
		C.corridor_name,
		B.seq,
		B.link_dir,
		EXTRACT(HOUR from A.tx) as hh,
		B.distance_km * AVG(1.0/A.pct_50) * 3600.0 AS tt_avg,
		B.distance_km / (PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY A.pct_50)) * 3600.0 AS tt_med,
		COUNT(A.pct_50) AS obs

FROM	 	here.ta A
INNER JOIN 	here_analysis.corridor_links B USING (link_dir)
INNER JOIN 	here_analysis.corridors C USING (corridor_id)
LEFT JOIN	ref.holiday E ON e.dt = A.tx::date

WHERE		A.tx::date IN ('2017-04-18','2017-04-19','2017-04-20','2017-04-25','2017-04-26','2017-04-27','2017-05-02','2017-05-03','2017-05-04') -- change this
		AND EXTRACT(dow FROM A.tx) IN (2,3,4)
		AND E.dt IS NULL
		AND C.group_id IN (99,100) -- Bloor St EB and WB defined routes
		AND EXTRACT(hour from A.tx) >= 7
		AND EXTRACT(hour from A.tx) <= 18
		
GROUP BY 	C.group_id,
		C.group_order,
		C.corridor_id,
		C.corridor_name,
		B.seq,
		B.link_dir,
		EXTRACT(HOUR from A.tx),
		B.distance_km
		
ORDER BY 	C.group_id,
		C.group_order,
		B.seq,
		EXTRACT(HOUR from A.tx)