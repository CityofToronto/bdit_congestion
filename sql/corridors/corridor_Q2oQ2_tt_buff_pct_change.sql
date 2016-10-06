SELECT corridor, direction, left(direction,1)||'B' as dir, period,
	to_char(100*(q2.tt_med - q1.tt_med)/q1.tt_med, 'FMSG990.0')||'%' AS median_tt,
	to_char(100*(q2.tt_95th - q1.tt_95th - (q2.tt_med - q1.tt_med))/(q1.tt_95th-q1.tt_med),'FMSG990.0')||'%' AS buffer_time
  FROM key_corridor_perf q1
  INNER JOIN key_corridor_perf q2 USING (corridor_id, daytype,period)
  INNER JOIN key_corridor_lookup USING (corridor_id)
  WHERE daytype = 'Midweek'  
  AND q1.quarter = '2014-04-01' AND q1.period IN ('AMPK','PMPK')
  AND q2.quarter = '2016-04-01' AND q2.period IN ('AMPK','PMPK')
ORDER BY corridor, direction;

