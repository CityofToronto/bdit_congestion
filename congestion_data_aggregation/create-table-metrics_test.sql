--Using all data
CREATE TABLE congestion.metrics_test AS

SELECT *,
avg_spd / baseline_spd AS tti,
(pct95_tt - avg_tt)/avg_tt AS bi

FROM (
SELECT a.segment_id, 
a.datetime_bin::time AS time_bin, 
COUNT(datetime_bin) AS num_bins, 
b.seg_length, 
AVG(a.segment_tt_avg_all) AS avg_tt,
PERCENTILE_CONT (0.95) WITHIN GROUP (ORDER BY a.segment_tt_avg_all ASC) AS pct95_tt,
b.seg_length / AVG(a.segment_tt_avg_all) * 3.6 AS avg_spd,
b.spd_baseline_10pct_all AS baseline_spd
FROM congestion.tt_segments_30min_test a
LEFT JOIN congestion.tt_segments_baseline_test b
USING (segment_id)
WHERE a.segment_tt_avg_all IS NOT NULL
GROUP BY segment_id, datetime_bin::time, b.seg_length, b.spd_baseline_10pct_all
ORDER BY segment_id, datetime_bin::time
) abc
