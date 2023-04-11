CREATE TABLE congestion.tt_corridor_baseline_v4_2019 AS 

WITH daytime AS
(SELECT corridor_id, 
PERCENTILE_CONT (0.10) WITHIN GROUP (ORDER BY corridor_tt_avg_hc ASC) AS tt_baseline_10pct,
PERCENTILE_CONT (0.15) WITHIN GROUP (ORDER BY corridor_tt_avg_hc ASC) AS tt_baseline_15pct,
PERCENTILE_CONT (0.20) WITHIN GROUP (ORDER BY corridor_tt_avg_hc ASC) AS tt_baseline_20pct,
PERCENTILE_CONT (0.25) WITHIN GROUP (ORDER BY corridor_tt_avg_hc ASC) AS tt_baseline_25pct,
PERCENTILE_CONT (0.25) WITHIN GROUP (ORDER BY corridor_tt_med_hc ASC) AS tt_baseline_25pct_med
FROM congestion.tt_corridor_30min_v4_2019
WHERE datetime_bin::time BETWEEN '07:00:00' AND '20:59:59'
GROUP BY corridor_id
),

overnight AS
(SELECT corridor_id, 
SUM(corridor_tt_avg_hc), COUNT(corridor_tt_avg_hc),
AVG(corridor_tt_avg_hc) AS tt_baseline_overnight,
SUM(corridor_tt_med_hc), COUNT(corridor_tt_med_hc),
AVG(corridor_tt_med_hc) AS tt_baseline_overnight_med
FROM congestion.tt_corridor_30min_v4_2019
WHERE datetime_bin::time >= '22:00:00' 
   OR datetime_bin::time < '06:00:00'
GROUP BY corridor_id
ORDER BY corridor_id)

SELECT daytime.corridor_id, corridor.length AS seg_length,
overnight.tt_baseline_overnight,
daytime.tt_baseline_10pct, daytime.tt_baseline_15pct, tt_baseline_20pct, tt_baseline_25pct, tt_baseline_25pct_med,
corridor.length / overnight.tt_baseline_overnight * 3.6 AS spd_baseline_overnight,
corridor.length / overnight.tt_baseline_overnight_med * 3.6 AS spd_baseline_overnight_med,
corridor.length / daytime.tt_baseline_10pct * 3.6 AS spd_baseline_10pct,
corridor.length / daytime.tt_baseline_15pct * 3.6 AS spd_baseline_15pct,
corridor.length / daytime.tt_baseline_20pct * 3.6 AS spd_baseline_20pct,
corridor.length / daytime.tt_baseline_25pct * 3.6 AS spd_baseline_25pct,
corridor.length / daytime.tt_baseline_25pct_med * 3.6 AS spd_baseline_25pct_med
FROM daytime 
JOIN overnight USING (corridor_id)
JOIN (select distinct corridor_id, length from congestion.corridor_segments_v4)corridor USING (corridor_id)