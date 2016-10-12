/*Step 2, calculating median and 95th percentile corridor travel times by corridor_id, timeperiod, daytype, month. Using all scores */
SELECT date_trunc('month',dt) as dt_mon, COALESCE(holiday,daytype) AS daytype, period, corridor_id, percentile_cont(0.5) WITHIN GROUP(ORDER BY corridor_tt) as tt_med, percentile_cont(0.95) WITHIN GROUP(ORDER BY corridor_tt) as tt_95th
INTO rdumas.key_corridor_perf_alldata
FROM rdumas.corridor_travel_times_alldata
INNER JOIN ref.timeperiod USING (time_15_continuous)
INNER JOIN ref.daytypes ON (isodow = EXTRACT('isodow' from dt))
LEFT OUTER JOIN ref.holiday USING (dt)
GROUP BY dt_mon, COALESCE(holiday, daytype), period, corridor_id