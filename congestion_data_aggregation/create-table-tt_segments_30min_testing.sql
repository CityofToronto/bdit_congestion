CREATE TABLE congestion.tt_segments_30min_test AS

WITH X AS
(
SELECT a.segment_id, a.link_dir, a.datetime_bin, a.link_length, a.spd_avg_all, a.spd_avg_hc,
a.link_length / a.spd_avg_all  * 3.6 AS link_tt_avg_all,
a.link_length / a.spd_avg_hc  * 3.6 AS link_tt_avg_hc,
b.length AS seg_length
FROM congestion.speeds_links_30min_test a
INNER JOIN congestion.segments_test b USING (segment_id)
***WHERE segment_id IN (1, 2)
GROUP BY segment_id, link_dir, datetime_bin, link_length, spd_avg_all, spd_avg_hc, b.length
ORDER BY segment_id, link_dir
)

, Y AS (
SELECT segment_id, datetime_bin, 
CASE WHEN SUM(link_length) >= 0.8 * seg_length THEN SUM(link_tt_avg_all) * seg_length / SUM(link_length)
END AS segment_tt_avg_all ,
SUM(link_length) / seg_length * 100 AS data_pct_all
FROM X
GROUP BY segment_id, datetime_bin, seg_length
ORDER BY segment_id, datetime_bin
)

, Z AS (
SELECT segment_id, datetime_bin, 
CASE WHEN SUM(link_length) >= 0.8 * seg_length THEN SUM(link_tt_avg_hc) * seg_length / SUM(link_length)
END AS segment_tt_avg_hc ,
SUM(link_length) / seg_length * 100 AS data_pct_hc
FROM X
WHERE link_tt_avg_hc IS NOT NULL
GROUP BY segment_id, datetime_bin, seg_length
ORDER BY segment_id, datetime_bin
)

SELECT Y.segment_id, Y.datetime_bin, 
segment_tt_avg_all, data_pct_all,
segment_tt_avg_hc, data_pct_hc
FROM Y
LEFT JOIN Z
USING (segment_id, datetime_bin)
