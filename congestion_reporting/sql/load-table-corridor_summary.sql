TRUNCATE TABLE here_analysis.corridor_summary;

INSERT INTO here_analysis.corridor_summary(corridor_id, year_bin, month_bin, hh, tt_avg, tt_50, tt_95)
SELECT	corridor_id,
	date_trunc('year', datetime_bin) as year_bin,
	NULL as month_bin,
	EXTRACT(HOUR FROM datetime_bin) AS hh,
	round(AVG(total_tt)::numeric,4) AS tt_avg,
	round(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_tt)::numeric,4) AS tt_50,
	round(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_tt)::numeric,4) AS tt_95
	
FROM	(	SELECT 		corridor_id, datetime_bin, SUM(tt_avg) AS total_tt
		FROM		here_analysis.corridor_links_15min
		WHERE		EXTRACT(hour FROM datetime_bin) IN (8,17) AND excluded = FALSE
		GROUP BY 	corridor_id, datetime_bin
	) A
GROUP BY	A.corridor_id,
		date_trunc('year', datetime_bin),
		EXTRACT(HOUR FROM datetime_bin);



INSERT INTO here_analysis.corridor_summary(corridor_id, year_bin, month_bin, hh, tt_avg, tt_50, tt_95)
SELECT	corridor_id,
	NULL as year_bin,
	date_trunc('month', datetime_bin) as month_bin,
	EXTRACT(HOUR FROM datetime_bin) AS hh,
	round(AVG(total_tt::numeric),4) AS tt_avg,
	round(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_tt)::numeric,4) AS tt_50,
	round(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_tt)::numeric,4) AS tt_95
	
FROM	(	SELECT 		corridor_id, datetime_bin, SUM(tt_avg) AS total_tt
		FROM		here_analysis.corridor_links_15min
		WHERE		EXTRACT(hour FROM datetime_bin) IN (8,17) AND excluded = FALSE
		GROUP BY 	corridor_id, datetime_bin
	) A
GROUP BY	A.corridor_id,
		date_trunc('month', datetime_bin),
		EXTRACT(HOUR FROM datetime_bin);

UPDATE 	here_analysis.corridor_summary A
SET 	tti = round(A.tt_50 / B.tt_ff,4)
FROM 	here_analysis.corridor_ff B
WHERE 	B.corridor_id = A.corridor_id;

UPDATE 	here_analysis.corridor_summary A
SET 	bti = round(A.tt_95 / A.tt_avg,4);