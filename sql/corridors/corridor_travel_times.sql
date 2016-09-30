/*Step one, sum up average travel time by corridor direction, for every 15 minute bin*/
SELECT dt, time_15_continuous, corridor, direction, SUM(avg_tt_min) as corridor_tt
INTO corridor_travel_times
FROM agg_extract_hour_subsample 
INNER JOIN key_corridors USING (tmc)
GROUP BY dt, time_15_continuous, corridor, direction