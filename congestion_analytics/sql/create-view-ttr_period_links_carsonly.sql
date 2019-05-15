-- DROP MATERIALIZED VIEW here_analysis.ttr_period_links_carsonly CASCADE;
CREATE MATERIALIZED VIEW here_analysis.ttr_period_links_carsonly AS
SELECT	A.link_dir,
	C.mth,
	D.period_id,
	avg(1/C.spd_avg) / avg(1/A.spd_avg) AS ttr

FROM	(
	SELECT 	link_dir,
		group_id,
		time_bin,
		spd_avg
	FROM 	here_analysis.monthly_averages_carsonly
	WHERE	mth = '2016-12-01'
	) A
INNER JOIN here_analysis.analysis_links_details B USING (link_dir)
INNER JOIN here_analysis.monthly_averages_carsonly C USING (link_dir, group_id, time_bin)
INNER JOIN here_analysis.analysis_periods D USING (group_id, time_bin)

GROUP BY A.link_dir, C.mth, D.period_id;