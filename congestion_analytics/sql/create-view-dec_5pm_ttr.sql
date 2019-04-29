CREATE MATERIALIZED VIEW here_analysis.dec_5pm_ttr AS
SELECT link_dir, A.spd_avg/B.spd_avg AS ttr
FROM (SELECT link_dir, 1.0/AVG(1.0/spd_avg) AS spd_avg FROM here_analysis.monthly_averages WHERE mth = '2015-12-01' AND time_bin IN ('17:00','17:30') AND group_id = 1 GROUP BY link_dir) A
INNER JOIN (SELECT link_dir, 1.0/AVG(1.0/spd_avg) AS spd_avg FROM here_analysis.monthly_averages WHERE mth = '2018-12-01' AND time_bin IN ('17:00','17:30') AND group_id = 1 GROUP BY link_dir) B USING (link_dir)
