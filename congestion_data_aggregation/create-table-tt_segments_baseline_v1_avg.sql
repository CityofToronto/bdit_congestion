CREATE TABLE congestion.tt_segments_baseline_v1_avg AS 

WITH daytime AS
(SELECT segment_id, 
PERCENTILE_CONT (0.10) WITHIN GROUP (ORDER BY segment_tt_avg_hc ASC) AS tt_baseline_10pct,
PERCENTILE_CONT (0.15) WITHIN GROUP (ORDER BY segment_tt_avg_hc ASC) AS tt_baseline_15pct,
PERCENTILE_CONT (0.20) WITHIN GROUP (ORDER BY segment_tt_avg_hc ASC) AS tt_baseline_20pct,
PERCENTILE_CONT (0.25) WITHIN GROUP (ORDER BY segment_tt_avg_hc ASC) AS tt_baseline_25pct
FROM congestion.tt_segments_30min_v1
WHERE datetime_bin::time BETWEEN '07:00:00' AND '20:59:59'
GROUP BY segment_id
),

overnight AS
(SELECT segment_id, 
SUM(segment_tt_avg_hc), COUNT(segment_tt_avg_hc),
AVG(segment_tt_avg_hc) AS tt_baseline_overnight
FROM congestion.tt_segments_30min_v1
WHERE datetime_bin::time >= '22:00:00' 
   OR datetime_bin::time < '06:00:00'
GROUP BY segment_id
ORDER BY segment_id)

SELECT daytime.segment_id, segments_v1.length AS seg_length,
overnight.tt_baseline_overnight,
segments_v1.length / overnight.tt_baseline_overnight * 3.6 AS spd_baseline_overnight,
daytime.tt_baseline_10pct, daytime.tt_baseline_15pct, tt_baseline_20pct, tt_baseline_25pct,
segments_v1.length / daytime.tt_baseline_10pct * 3.6 AS spd_baseline_10pct,
segments_v1.length / daytime.tt_baseline_15pct * 3.6 AS spd_baseline_15pct,
segments_v1.length / daytime.tt_baseline_20pct * 3.6 AS spd_baseline_20pct,
segments_v1.length / daytime.tt_baseline_25pct * 3.6 AS spd_baseline_25pct
FROM daytime 
JOIN overnight USING (segment_id)
JOIN congestion.segments_v1 USING (segment_id)



--for verification (daytime)
SELECT * 
FROM congestion.tt_segments_30min_v1
WHERE datetime_bin::time BETWEEN '07:00:00' AND '20:59:59'
--AND segment_tt_avg_all IS NOT NULL (not neccessary)
ORDER BY segment_id, segment_tt_avg_hc