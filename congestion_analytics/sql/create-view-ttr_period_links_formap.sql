CREATE OR REPLACE VIEW here_analysis.ttr_period_links_ampeak AS 
WITH X AS (
	SELECT 		link_dir,
			SUM(CASE WHEN mth = '2018-09-01' THEN ttr ELSE 0 END)/SUM(CASE WHEN mth = '2016-09-01' THEN ttr ELSE 0 END) AS ttr
	FROM 		here_analysis.ttr_period_links 
	WHERE 		mth IN ('2018-09-01','2016-09-01') AND period_id = 1
	GROUP BY 	link_dir
	HAVING		COUNT(1) = 2
)

SELECT link_dir, ttr, description
FROM X
INNER JOIN here_analysis.ttr_link_groups B ON B.ttr_range @> X.ttr;


CREATE OR REPLACE VIEW here_analysis.ttr_period_links_pmpeak AS 
WITH X AS (
	SELECT 		link_dir,
			SUM(CASE WHEN mth = '2018-09-01' THEN ttr ELSE 0 END)/SUM(CASE WHEN mth = '2016-09-01' THEN ttr ELSE 0 END) AS ttr
	FROM 		here_analysis.ttr_period_links 
	WHERE 		mth IN ('2018-09-01','2016-09-01') AND period_id = 2
	GROUP BY 	link_dir
	HAVING		COUNT(1) = 2
)

SELECT link_dir, ttr, description
FROM X
INNER JOIN here_analysis.ttr_link_groups B ON B.ttr_range @> X.ttr;