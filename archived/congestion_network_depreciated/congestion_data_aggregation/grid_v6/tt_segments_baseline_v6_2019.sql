CREATE TABLE congestion.tt_segments_baseline_v6_2019 AS 

WITH daytime AS
(SELECT segment_id, 
PERCENTILE_CONT (0.10) WITHIN GROUP (ORDER BY segment_tt_avg_hc ASC) AS tt_baseline_10pct,
PERCENTILE_CONT (0.15) WITHIN GROUP (ORDER BY segment_tt_avg_hc ASC) AS tt_baseline_15pct,
PERCENTILE_CONT (0.20) WITHIN GROUP (ORDER BY segment_tt_avg_hc ASC) AS tt_baseline_20pct,
PERCENTILE_CONT (0.25) WITHIN GROUP (ORDER BY segment_tt_avg_hc ASC) AS tt_baseline_25pct,
PERCENTILE_CONT (0.25) WITHIN GROUP (ORDER BY segment_tt_med_hc ASC) AS tt_baseline_25pct_med

FROM congestion.tt_segments_30min_v6_2019
WHERE datetime_bin::time BETWEEN '07:00:00' AND '20:59:59'
GROUP BY segment_id
),

overnight AS
(SELECT segment_id, 
AVG(segment_tt_avg_hc) AS tt_baseline_overnight,
AVG(segment_tt_med_hc) AS tt_baseline_overnight_med
FROM congestion.tt_segments_30min_v6_2019
WHERE datetime_bin::time >= '22:00:00' 
   OR datetime_bin::time < '06:00:00'
GROUP BY segment_id
ORDER BY segment_id)

SELECT daytime.segment_id, segments.length AS seg_length,
tt_baseline_overnight, 
tt_baseline_overnight_med,
tt_baseline_10pct, 
tt_baseline_15pct, 
tt_baseline_20pct,
tt_baseline_25pct,
tt_baseline_25pct_med,

segments.length / overnight.tt_baseline_overnight * 3.6 AS spd_baseline_overnight,
segments.length / overnight.tt_baseline_overnight_med * 3.6 AS spd_baseline_overnight_med,
segments.length / daytime.tt_baseline_10pct * 3.6 AS spd_baseline_10pct,
segments.length / daytime.tt_baseline_15pct * 3.6 AS spd_baseline_15pct,
segments.length / daytime.tt_baseline_20pct * 3.6 AS spd_baseline_20pct,
segments.length / daytime.tt_baseline_25pct * 3.6 AS spd_baseline_25pct,
segments.length / daytime.tt_baseline_25pct_med * 3.6 AS spd_baseline_25pct_med

FROM daytime 
JOIN overnight USING (segment_id)
JOIN congestion.segments_v6 segments USING (segment_id)