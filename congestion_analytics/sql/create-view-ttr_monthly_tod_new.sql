CREATE MATERIALIZED VIEW here_analysis.ttr_monthly_tod_new AS 
SELECT 		A.mth,
		A.period_id,
		SUM(A.ttr*ST_Length(B.geom))/SUM(ST_Length(B.geom)) AS ttr

FROM		here_analysis.ttr_period_links A
INNER JOIN	here_analysis.here_gis B USING (link_dir)

GROUP BY	A.mth,
		A.period_id