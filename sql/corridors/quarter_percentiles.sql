SELECT date_trunc('quarter',dt) as quarter, COALESCE(holiday,daytype) AS daytype, timeperiod, tmc, percentile_cont(0.5) WITHIN GROUP(ORDER BY avg_tt_min) as tt_med, percentile_cont(0.95) WITHIN GROUP(ORDER BY avg_tt_min) as tt_95th
INTO rdumas.key_corridor_perf
FROM rdumas.agg_extract_hour_subsample a
INNER JOIN key_corridors USING (tmc)
INNER JOIN ref.timeperiod USING (time_15_continuous)
INNER JOIN ref.daytypes ON (isodow = EXTRACT('isodow' from dt))
LEFT OUTER JOIN ref.holiday USING (dt)
GROUP BY quarter, holiday, daytype, timeperiod, tmc