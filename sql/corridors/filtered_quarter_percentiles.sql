/*Step 3: Filter by daytype and timeperiod*/
SELECT quarter::date, corridor, direction, period, tt_med as median_tt, tt_95th - tt_med as buffer_time
FROM key_corridor_perf 
WHERE daytype = 'Midweek' and period IN ('AMPK','PMPK')
ORDER BY corridor, direction, period, quarter
