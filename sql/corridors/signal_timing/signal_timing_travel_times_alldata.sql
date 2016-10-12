/*Step one, sum up average travel time by corridor direction, for every 15 minute bin*/
DROP TABLE IF EXISTS corridors_signal_timing_tt;
SELECT dt, time_15_continuous, signal_timing_id, SUM(avg_tt_min) as corridor_tt
INTO corridors_signal_timing_tt
FROM analysis.agg_extract_hour_alldata  
INNER JOIN corridors_signal_timing USING (tmc)
GROUP BY dt, time_15_continuous, signal_timing_id;

