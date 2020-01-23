CREATE TABLE congestion.metrics_test AS

SELECT segment_id, time_bin, num_bins,
avg_tt / baseline_tt AS tti,
(pct95_tt - avg_tt)/avg_tt AS biff

FROM (
SELECT a.segment_id, 
a.datetime_bin::time AS time_bin, 
COUNT(datetime_bin) AS num_bins, 
b.seg_length, 
AVG(a.segment_tt_avg_all) AS avg_tt,
PERCENTILE_CONT (0.95) WITHIN GROUP (ORDER BY a.segment_tt_avg_all ASC) AS pct95_tt,
b.tt_baseline_10pct_all AS baseline_tt
FROM congestion.tt_segments_30min_test a
LEFT JOIN congestion.tt_segments_baseline_test b
USING (segment_id)
WHERE a.segment_tt_avg_all IS NOT NULL
GROUP BY segment_id, datetime_bin::time, b.seg_length, b.tt_baseline_10pct_all
ORDER BY segment_id, datetime_bin::time
) abc
