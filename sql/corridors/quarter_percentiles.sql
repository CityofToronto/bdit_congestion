/*Step 2, calculating median and 95th percentile corridor travel times by direction, timeperiod, daytype, and quarter */
SELECT date_trunc('quarter',dt) as quarter, COALESCE(holiday,daytype) AS daytype, period, corridor, direction, percentile_cont(0.5) WITHIN GROUP(ORDER BY corridor_tt) as tt_med, percentile_cont(0.95) WITHIN GROUP(ORDER BY corridor_tt) as tt_95th
INTO rdumas.key_corridor_perf
FROM rdumas.corridor_travel_times
INNER JOIN ref.timeperiod USING (time_15_continuous)
INNER JOIN ref.daytypes ON (isodow = EXTRACT('isodow' from dt))
LEFT OUTER JOIN ref.holiday USING (dt)
GROUP BY quarter, COALESCE(holiday, daytype), period, corridor, direction