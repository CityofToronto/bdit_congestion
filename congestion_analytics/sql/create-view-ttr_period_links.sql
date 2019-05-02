DROP MATERIALIZED VIEW here_analysis.ttr_period_links CASCADE;
CREATE MATERIALIZED VIEW here_analysis.ttr_period_links AS
SELECT	A.link_dir,
	C.mth,
	D.period_id,
	avg(1/C.spd_avg) / avg(1/A.spd_avg) AS ttr

FROM	(
	SELECT 	link_dir,
		group_id,
		time_bin,
		spd_avg
	FROM 	here_analysis.monthly_averages
	WHERE	mth = '2014-01-01'
	) A
INNER JOIN here_analysis.analysis_links_details B USING (link_dir)
INNER JOIN here_analysis.monthly_averages C USING (link_dir, group_id, time_bin)
INNER JOIN here_analysis.analysis_periods D USING (group_id, time_bin)

GROUP BY A.link_dir, C.mth, D.period_id;