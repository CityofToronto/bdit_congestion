CREATE TABLE congestion.data_sufficient_5min AS

WITH days AS (
SELECT link_dir, COUNT(DISTINCT tx::date) AS day_count, (datetime_bin(tx,30))::time AS time_bin
FROM congestion.data_fall2019_5min 
INNER JOIN congestion.segment_links USING (link_dir)
GROUP BY link_dir, time_bin
	),
	conf AS (
SELECT link_dir, COUNT(DISTINCT tx::date) AS conf_day_count, (datetime_bin(tx,30))::time AS time_bin
FROM congestion.data_fall2019_5min 
INNER JOIN congestion.segment_links USING (link_dir)
WHERE confidence >= 30
GROUP BY link_dir, time_bin
	)	
SELECT days.link_dir, days.time_bin, days.day_count, conf.conf_day_count
FROM days
LEFT JOIN conf USING (link_dir, time_bin)
ORDER BY link_dir, time_bin
