/*Step 2, calculating median and 95th percentile corridor travel times by signal_timing_id, timeperiod, daytype, month. Using all scores */
SELECT date_trunc('week',dt) as dt_week, COALESCE(holiday,daytype) AS daytype, period, signal_timing_id, percentile_cont(0.5) WITHIN GROUP(ORDER BY corridor_tt) as tt_med
INTO rdumas.signal_timing_perf_alldata
FROM rdumas.corridors_signal_timing_tt
INNER JOIN ref.timeperiod USING (time_15_continuous)
INNER JOIN ref.daytypes ON (isodow = EXTRACT('isodow' from dt))
LEFT OUTER JOIN ref.holiday USING (dt)
GROUP BY dt_week, COALESCE(holiday, daytype), period, signal_timing_id