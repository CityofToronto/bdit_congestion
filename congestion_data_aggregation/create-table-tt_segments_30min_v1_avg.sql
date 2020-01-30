CREATE TABLE congestion.tt_segments_30min_v1_avg AS

WITH X AS
(
SELECT a.segment_id, a.link_dir, a.datetime_bin, a.link_length, 
a.spd_avg_all, a.spd_avg_hc,
a.spd_med_all, a.spd_med_hc,
a.link_length / a.spd_avg_all  * 3.6 AS link_tt_avg_all,
a.link_length / a.spd_avg_hc  * 3.6 AS link_tt_avg_hc,
a.link_length / a.spd_med_all  * 3.6 AS link_tt_med_all,
a.link_length / a.spd_med_hc  * 3.6 AS link_tt_med_hc,
b.length AS seg_length
FROM congestion.speeds_links_30min_v1_avg a
INNER JOIN congestion.segments_v1 b 
USING (segment_id)
GROUP BY segment_id, link_dir, datetime_bin, link_length, spd_avg_all, spd_avg_hc, spd_med_all, spd_med_hc, b.length
ORDER BY segment_id, link_dir
)

, Y AS ( --all=all confidence level 
SELECT segment_id, datetime_bin, 
CASE WHEN SUM(link_length) >= 0.8 * seg_length 
THEN SUM(link_tt_avg_all) * seg_length / SUM(link_length)
END AS segment_tt_avg_all ,
CASE WHEN SUM(link_length) >= 0.8 * seg_length 
THEN SUM(link_tt_med_all) * seg_length / SUM(link_length)
END AS segment_tt_med_all ,
SUM(link_length) / seg_length * 100 AS data_pct_all
FROM X
GROUP BY segment_id, datetime_bin, seg_length
ORDER BY segment_id, datetime_bin
)

, Z AS ( --hc=high confidence >= 30
SELECT segment_id, datetime_bin, 
CASE WHEN SUM(link_length) >= 0.8 * seg_length 
THEN SUM(link_tt_avg_hc) * seg_length / SUM(link_length)
END AS segment_tt_avg_hc ,
CASE WHEN SUM(link_length) >= 0.8 * seg_length 
THEN SUM(link_tt_med_hc) * seg_length / SUM(link_length)
END AS segment_tt_med_hc ,
SUM(link_length) / seg_length * 100 AS data_pct_hc
FROM X
WHERE link_tt_avg_hc IS NOT NULL
AND link_tt_med_hc IS NOT NULL
GROUP BY segment_id, datetime_bin, seg_length
ORDER BY segment_id, datetime_bin
)

SELECT Y.segment_id, Y.datetime_bin, 
segment_tt_avg_all, segment_tt_med_all, data_pct_all,
segment_tt_avg_hc, segment_tt_med_hc, data_pct_hc
FROM Y
LEFT JOIN Z
USING (segment_id, datetime_bin)
