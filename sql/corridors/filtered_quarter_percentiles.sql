SELECT quarter::date, corridor, direction, period, SUM(tt_med) as median_tt, SUM(tt_95th) - SUM(tt_med) as buffer_time
FROM key_corridor_perf 
WHERE daytype = 'Midweek' and period IN ('AMPK','PMPK')
GROUP BY quarter, corridor, direction, period
ORDER BY corridor, direction, period, quarter
