CREATE TABLE congestion.metrics_v4_2019_af AS

SELECT segment_id, time_bin, num_bins,
avg_tt, avg_tt_corr, baseline_tt, baseline_tt_corr, baseline_tt_med, baseline_tt_med_corr, pct95_tt,
avg_tt / baseline_tt AS tti,
(pct95_tt - avg_tt)/avg_tt AS bi

FROM (
SELECT a.segment_id, 
a.datetime_bin::time AS time_bin, 
COUNT(datetime_bin) AS num_bins, 
b.seg_length, 
AVG(a.segment_tt_avg_hc) AS avg_tt,
AVG(a.segment_tt_avg_hc_corr) AS avg_tt_corr,
AVG(a.segment_tt_med_hc) AS med_tt,
AVG(a.segment_tt_med_hc_corr) AS med_tt_corr,	
PERCENTILE_CONT (0.95) WITHIN GROUP (ORDER BY a.segment_tt_avg_hc ASC) AS pct95_tt,
PERCENTILE_CONT (0.95) WITHIN GROUP (ORDER BY a.segment_tt_avg_hc_corr ASC) AS pct95_tt_corr,
b.tt_baseline_25pct AS baseline_tt,
b.tt_baseline_25pct_corr AS baseline_tt_corr,
b.tt_baseline_25pct_med AS baseline_tt_med,
b.tt_baseline_25pct_med_corr AS baseline_tt_med_corr	
	
FROM congestion.tt_segments_30min_v4_2019_af a
LEFT JOIN congestion.tt_segments_baseline_v4_2019_af b
USING (segment_id)
WHERE a.segment_tt_avg_all IS NOT NULL
GROUP BY segment_id, datetime_bin::time, b.seg_length, b.tt_baseline_25pct
ORDER BY segment_id, datetime_bin::time
) abc