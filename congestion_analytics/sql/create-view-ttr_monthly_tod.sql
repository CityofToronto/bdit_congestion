DROP MATERIALIZED VIEW here_analysis.ttr_monthly_tod CASCADE;
CREATE MATERIALIZED VIEW here_analysis.ttr_monthly_tod AS
SELECT 		B.mth, 
		group_id, 
		time_bin,
		SUM((1/B.spd_avg) / (1/A.spd_avg) * C.length_m) / SUM(C.length_m) AS ttr_weighted
FROM	 	(	SELECT * 
			FROM here_analysis.monthly_averages 
			WHERE mth = '2016-07-01'
		) AS A
INNER JOIN 	here_analysis.monthly_averages B USING (link_dir, group_id, time_bin)
INNER JOIN 	here_analysis.analysis_links_details C USING (link_dir)
GROUP BY 	B.mth, group_id, time_bin
ORDER BY 	B.mth;