-- metric table for corridor 
CREATE TABLE congestion.corridor_metrics_v4_2019 as 
SELECT corridor_id, time_bin, num_bins,
avg_tt, baseline_tt, pct95_tt,
avg_tt / baseline_tt AS tti,
med_tt/baseline_tt_med as tti_median,
(pct95_tt - avg_tt)/avg_tt AS bi

FROM (
SELECT a.corridor_id, 
a.datetime_bin::time AS time_bin, 
COUNT(datetime_bin) AS num_bins, 
b.seg_length, 
AVG(a.corridor_tt_avg_hc) AS avg_tt,
avg(a.corridor_tt_med_hc) AS med_tt, 	
PERCENTILE_CONT (0.95) WITHIN GROUP (ORDER BY a.corridor_tt_avg_hc ASC) AS pct95_tt,
b.tt_baseline_25pct AS baseline_tt,
b.tt_baseline_25pct_med as baseline_tt_med	
FROM congestion.tt_corridor_30min_v4_2019 a
LEFT JOIN congestion.tt_corridor_baseline_v4_2019 b
USING (corridor_id)
WHERE a.corridor_tt_avg_all IS NOT NULL
GROUP BY corridor_id, datetime_bin::time, b.seg_length, b.tt_baseline_25pct, b.tt_baseline_25pct_med
ORDER BY corridor_id, datetime_bin::time
) abc


