--returned 561,599
--took 44.5seconds

CREATE TABLE congestion.metrics_v1_avg AS

SELECT segment_id, time_bin, num_bins,
avg_tt, baseline_tt, pct95_tt,
avg_tt / baseline_tt AS tti,
(pct95_tt - avg_tt)/avg_tt AS bi

FROM (
SELECT a.segment_id, 
a.datetime_bin::time AS time_bin, 
COUNT(datetime_bin) AS num_bins, 
b.seg_length, 
AVG(a.segment_tt_avg_hc) AS avg_tt,
PERCENTILE_CONT (0.95) WITHIN GROUP (ORDER BY a.segment_tt_avg_hc ASC) AS pct95_tt,
b.tt_baseline_25pct AS baseline_tt
FROM congestion.tt_segments_30min_v1_avg a
LEFT JOIN congestion.tt_segments_baseline_v1_avg b
USING (segment_id)
WHERE a.segment_tt_avg_all IS NOT NULL
GROUP BY segment_id, datetime_bin::time, b.seg_length, b.tt_baseline_25pct
ORDER BY segment_id, datetime_bin::time
) abc
