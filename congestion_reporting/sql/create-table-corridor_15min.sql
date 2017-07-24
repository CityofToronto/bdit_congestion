DROP TABLE IF EXISTS here_analysis.corridor_15min;

CREATE TABLE here_analysis.corridor_15min (
	corridor_id integer,
	datetime_bin timestamp without time zone,
	total_tt numeric,
	pct_real numeric);

INSERT INTO	here_analysis.corridor_15min (corridor_id, datetime_bin, total_tt, pct_real)
SELECT 		corridor_id, datetime_bin, SUM(tt_avg) AS total_tt, SUM(CASE WHEN estimated = FALSE THEN 1 ELSE 0 END)*1.0/COUNT(*)*1.0 AS pct_real
		FROM		here_analysis.corridor_links_15min
		WHERE		EXTRACT(hour FROM datetime_bin) IN (8,17) AND excluded = FALSE
		GROUP BY 	corridor_id, datetime_bin;