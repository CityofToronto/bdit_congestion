--Testing using `congestion.segment_links_v1` and `congestion.segments_v1`
--which has 11918 segments and 38336 links
--took 3 hr 4 min
--returned 71,697,882 rows

CREATE TABLE congestion.speeds_links_30min_v1_avg AS

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
FROM congestion.segment_links_v1 a
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
FROM congestion.segment_links_v1 a
INNER JOIN congestion.data_fall2019_5min b
USING (link_dir)
WHERE confidence >= 30
GROUP BY a.segment_id, a.link_dir, datetime_bin, b.length
	)  Y

USING (segment_id, link_dir, datetime_bin, length)
ORDER BY segment_id, link_dir, datetime_bin

