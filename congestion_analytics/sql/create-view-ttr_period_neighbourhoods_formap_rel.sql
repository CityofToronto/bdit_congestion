CREATE OR REPLACE VIEW here_analysis.ttr_period_neighbourhood_ampeak_rel AS 
SELECT 		A.area_name,
		A.geom,
		SUM(CASE WHEN mth = '2018-09-01' THEN A.ttr/B.ttr ELSE 0 END)/SUM(CASE WHEN mth = '2016-09-01' THEN A.ttr/B.ttr ELSE 0 END) AS ttr
FROM 		here_analysis.ttr_period_neighbourhood A
INNER JOIN 	here_analysis.ttr_monthly_tod_new B USING (mth, period_id)
WHERE 		A.mth IN ('2018-09-01','2016-09-01') AND A.period_id = 1
GROUP BY 	area_name, geom;

CREATE OR REPLACE VIEW here_analysis.ttr_period_neighbourhood_pmpeak_rel AS 
SELECT 		A.area_name,
		A.geom,
		SUM(CASE WHEN mth = '2018-09-01' THEN A.ttr/B.ttr ELSE 0 END)/SUM(CASE WHEN mth = '2016-09-01' THEN A.ttr/B.ttr ELSE 0 END) AS ttr
FROM 		here_analysis.ttr_period_neighbourhood A
INNER JOIN 	here_analysis.ttr_monthly_tod_new B USING (mth, period_id)
WHERE 		A.mth IN ('2018-09-01','2016-09-01') AND A.period_id = 2
GROUP BY 	area_name, geom;

CREATE OR REPLACE VIEW here_analysis.ttr_period_neighbourhood_wenightpeak_rel AS 
SELECT 		A.area_name,
		A.geom,
		(SUM(CASE WHEN mth = '2018-09-01' THEN A.ttr/B.ttr ELSE 0 END)-0.00001)/(SUM(CASE WHEN mth = '2016-09-01' THEN A.ttr/B.ttr ELSE 0 END)-0.00001) AS ttr
FROM 		here_analysis.ttr_period_neighbourhood A
INNER JOIN 	here_analysis.ttr_monthly_tod_new B USING (mth, period_id)
WHERE 		A.mth IN ('2018-09-01','2016-09-01') AND A.period_id = 3
GROUP BY 	area_name, geom;