--Testing using `congestion.segment_links_v3` and `congestion.segments_v3`
--which has 12458 segments and 38976 links
--took 1hr 30min
--returned 72,923,647 rows

CREATE TABLE congestion.speeds_links_30min_v3 AS

SELECT X.segment_id, X.link_dir, X.length AS link_length, X.datetime_bin, 
X.spd_avg_all, Y.spd_avg_hc,
X.spd_med_all, Y.spd_med_hc,
X.count_all, Y.count_hc

FROM
(
SELECT a.segment_id, a.link_dir, 
(datetime_bin(b.tx,30)) AS datetime_bin,
harmean(mean) AS spd_avg_all,
harmean(pct_50) AS spd_med_all, 
COUNT (DISTINCT b.tx) AS count_all, b.length
FROM congestion.segment_links_v3 a
INNER JOIN congestion.data_fall2019_5min b
USING (link_dir)
GROUP BY a.segment_id, a.link_dir, datetime_bin, b.length
) X

LEFT JOIN

(
SELECT a.segment_id, a.link_dir, 
(datetime_bin(b.tx,30)) AS datetime_bin,
harmean(mean) AS spd_avg_hc,
harmean(pct_50) AS spd_med_hc, 
COUNT (DISTINCT b.tx)  AS count_hc, b.length
FROM congestion.segment_links_v3 a
INNER JOIN congestion.data_fall2019_5min b
USING (link_dir)
WHERE confidence >= 30
GROUP BY a.segment_id, a.link_dir, datetime_bin, b.length
	)  Y

USING (segment_id, link_dir, datetime_bin, length)
ORDER BY segment_id, link_dir, datetime_bin
