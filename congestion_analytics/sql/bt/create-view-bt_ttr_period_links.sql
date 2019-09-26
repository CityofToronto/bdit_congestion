CREATE OR REPLACE VIEW here_analysis.bt_ttr_period_links AS
SELECT	row_number() OVER (ORDER BY analysis_id) AS id,
	A.analysis_id,
	B.mth,
	C.period_id,
	AVG(B.tt_avg) /AVG( A.tt_avg) AS ttr,
	(avg(B.tt_avg) - avg(A.tt_avg)) / avg(A.tt_avg) AS pct_change

FROM	(
	SELECT 	analysis_id,
		group_id,
		time_bin,
		tt_avg
	FROM 	here_analysis.bt_monthly_averages
	WHERE	mth = '2017-10-01'
	) A
INNER JOIN here_analysis.bt_monthly_averages B USING (analysis_id, group_id, time_bin)
INNER JOIN here_analysis.analysis_periods C USING (group_id, time_bin)

GROUP BY A.analysis_id, B.mth, C.period_id;