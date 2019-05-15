CREATE OR REPLACE VIEW here_analysis.bt_ttr_period_links AS
SELECT	A.analysis_id,
	B.mth,
	C.period_id,
	avg(B.tt_avg) / avg(A.tt_avg) AS ttr

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