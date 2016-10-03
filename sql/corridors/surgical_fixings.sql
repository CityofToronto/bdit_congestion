/*Step one, sum up average travel time by corridor direction, for every 15 minute bin*/
DELETE FROM corridor_travel_times
WHERE corridor = 'Lake Shore Blvd Park Lawn to BC';

INSERT INTO corridor_travel_times
SELECT dt, time_15_continuous, corridor, direction, SUM(avg_tt_min) as corridor_tt
FROM agg_extract_hour_subsample 
INNER JOIN key_corridors USING (tmc)
WHERE corridor= 'Lake Shore Blvd Park Lawn to BC'
GROUP BY dt, time_15_continuous, corridor, direction;

DELETE FROM key_corridor_perf
WHERE corridor = 'Lake Shore Blvd Park Lawn to BC';

INSERT INTO key_corridor_perf
SELECT date_trunc('quarter',dt) as quarter, COALESCE(holiday,daytype) AS daytype, period, corridor, direction, percentile_cont(0.5) WITHIN GROUP(ORDER BY corridor_tt) as tt_med, percentile_cont(0.95) WITHIN GROUP(ORDER BY corridor_tt) as tt_95th
FROM rdumas.corridor_travel_times
INNER JOIN ref.timeperiod USING (time_15_continuous)
INNER JOIN ref.daytypes ON (isodow = EXTRACT('isodow' from dt))
LEFT OUTER JOIN ref.holiday USING (dt)
WHERE corridor = 'Lake Shore Blvd Park Lawn to BC'
GROUP BY quarter, COALESCE(holiday, daytype), period, corridor, direction;
