/*Step one, sum up average travel time by corridor direction, for every 15 minute bin*/
DROP TABLE IF EXISTS corridor_travel_times_alldata;
SELECT dt, time_15_continuous, corridor_id, SUM(avg_tt_min) as corridor_tt
INTO corridor_travel_times_alldata
FROM analysis.agg_extract_hour_alldata  
INNER JOIN key_corridors USING (tmc)
GROUP BY dt, time_15_continuous, corridor_id