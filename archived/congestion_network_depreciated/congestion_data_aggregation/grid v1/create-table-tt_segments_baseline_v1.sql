CREATE TABLE congestion.tt_segments_baseline_v1 AS 

WITH daytime AS
(SELECT segment_id, 
PERCENTILE_CONT (0.1) WITHIN GROUP (ORDER BY segment_tt_avg_all ASC) AS tt_baseline_10pct_all,
PERCENTILE_CONT (0.1) WITHIN GROUP (ORDER BY segment_tt_avg_hc ASC) AS tt_baseline_10pct_hc
FROM congestion.tt_segments_30min_v1
WHERE datetime_bin::time BETWEEN '07:00:00' AND '20:59:59'
GROUP BY segment_id
),

overnight AS
(SELECT segment_id, 
SUM(segment_tt_avg_all), COUNT(segment_tt_avg_all),
AVG(segment_tt_avg_all) AS tt_baseline_overnight_all,
SUM(segment_tt_avg_hc), COUNT(segment_tt_avg_hc),
AVG(segment_tt_avg_hc) AS tt_baseline_overnight_hc
FROM congestion.tt_segments_30min_v1
WHERE datetime_bin::time >= '23:00:00' 
   OR datetime_bin::time < '07:00:00'
GROUP BY segment_id
ORDER BY segment_id)

SELECT daytime.segment_id, segments_v1.length AS seg_length,
overnight.tt_baseline_overnight_all, overnight.tt_baseline_overnight_hc,
segments_v1.length / overnight.tt_baseline_overnight_all * 3.6 AS spd_baseline_overnight_all,
segments_v1.length / overnight.tt_baseline_overnight_hc * 3.6 AS spd_baseline_overnight_hc,
daytime.tt_baseline_10pct_all, daytime.tt_baseline_10pct_hc,
segments_v1.length / daytime.tt_baseline_10pct_all * 3.6 AS spd_baseline_10pct_all,
segments_v1.length / daytime.tt_baseline_10pct_hc * 3.6 AS spd_baseline_10pct_hc
FROM daytime 
JOIN overnight USING (segment_id)
JOIN congestion.segments_v1 USING (segment_id)



--for verification (daytime)
SELECT * 
FROM congestion.tt_segments_30min_v1
WHERE datetime_bin::time BETWEEN '07:00:00' AND '20:59:59'
--AND segment_tt_avg_all IS NOT NULL (not neccessary)
ORDER BY segment_id, segment_tt_avg_hc