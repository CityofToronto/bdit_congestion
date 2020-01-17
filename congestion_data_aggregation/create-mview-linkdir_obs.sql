CREATE MATERIALIZED ViEW congestion.linkdir_obs AS

SELECT ROW_NUMBER() OVER (ORDER BY SUM(day_count) DESC) AS priority, 
link_dir, SUM(day_count) AS total_count
FROM 
(
SELECT link_dir, COUNT(DISTINCT tx::date) AS day_count, (datetime_bin(tx,30))::time AS time_bin
FROM congestion.data_fall2019_5min 
INNER JOIN congestion.routing_grid USING (link_dir)
GROUP BY link_dir, time_bin
) a
GROUP BY link_dir


--this query is muchhhh slower
WITH X AS (
SELECT link_dir, COUNT(DISTINCT tx::date) AS day_count, (datetime_bin(tx,30))::time AS time_bin
FROM congestion.data_fall2019_5min 
INNER JOIN congestion.segment_link_test USING (link_dir)
GROUP BY link_dir, time_bin
)
SELECT ROW_NUMBER() OVER (ORDER BY SUM(day_count) DESC) AS priority, link_dir, sum(day_count) AS total_count
FROM X
GROUP BY link_dir
