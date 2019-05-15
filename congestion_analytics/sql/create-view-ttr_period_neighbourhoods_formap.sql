CREATE OR REPLACE VIEW here_analysis.ttr_period_neighbourhood_ampeak AS 
 SELECT ttr_period_neighbourhood.area_name,
    ttr_period_neighbourhood.geom,
    SUM(CASE WHEN mth = '2018-07-01' THEN ttr ELSE 0 END)/SUM(CASE WHEN mth = '2016-07-01' THEN ttr ELSE 0 END) AS ttr
   FROM here_analysis.ttr_period_neighbourhood
  WHERE ttr_period_neighbourhood.mth IN ('2018-07-01','2016-07-01') AND ttr_period_neighbourhood.period_id = 1
	GROUP BY area_name, geom;

CREATE OR REPLACE VIEW here_analysis.ttr_period_neighbourhood_pmpeak AS 
 SELECT ttr_period_neighbourhood.area_name,
    ttr_period_neighbourhood.geom,
    SUM(CASE WHEN mth = '2018-07-01' THEN ttr ELSE 0 END)/SUM(CASE WHEN mth = '2016-07-01' THEN ttr ELSE 0 END) AS ttr
   FROM here_analysis.ttr_period_neighbourhood
  WHERE ttr_period_neighbourhood.mth IN ('2018-07-01','2016-07-01') AND ttr_period_neighbourhood.period_id = 2
	GROUP BY area_name, geom;

CREATE OR REPLACE VIEW here_analysis.ttr_period_neighbourhood_wenight AS 
 SELECT ttr_period_neighbourhood.area_name,
    ttr_period_neighbourhood.geom,
    SUM(CASE WHEN mth = '2018-07-01' THEN ttr ELSE 0 END)/SUM(CASE WHEN mth = '2016-07-01' THEN ttr ELSE 0 END) AS ttr
   FROM here_analysis.ttr_period_neighbourhood
  WHERE ttr_period_neighbourhood.mth IN ('2018-07-01','2016-07-01') AND ttr_period_neighbourhood.period_id = 3
	GROUP BY area_name, geom;