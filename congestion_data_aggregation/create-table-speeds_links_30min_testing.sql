--Testing using `congestion.segment_link_test` and `congestion.segments_test`
--which has 26 segments and 265 links
--526770 rows for total number of 30min bin
--323322 rows for those with conf >= 30

CREATE TABLE congestion.speeds_links_30min_test AS

SELECT X.segment_id, X.link_dir, X.length AS link_length, X.datetime_bin, 
X.spd_avg_all, Y.spd_avg_hc,
X.count_all, Y.count_hc

FROM
(
SELECT a.segment_id, a.link_dir, (datetime_bin(b.tx,30)) AS datetime_bin,
harmean(pct_50) AS spd_avg_all, COUNT (DISTINCT b.tx) AS count_all, b.length
FROM congestion.segment_link_test a
INNER JOIN congestion.data_fall2019_5min b
USING (link_dir)
GROUP BY a.segment_id, a.link_dir, datetime_bin, b.length
) X

LEFT JOIN

(
SELECT a.segment_id, a.link_dir, (datetime_bin(b.tx,30)) AS datetime_bin,
harmean(pct_50) AS spd_avg_hc, COUNT (distinct (datetime_bin(b.tx,30)) )  AS count_hc, b.length
FROM congestion.segment_link_test a
INNER JOIN congestion.data_fall2019_5min b
USING (link_dir)
WHERE confidence >= 30
GROUP BY a.segment_id, a.link_dir, datetime_bin, b.length
	)  Y

USING (segment_id, link_dir, datetime_bin, length)
ORDER BY segment_id, link_dir, datetime_bin
