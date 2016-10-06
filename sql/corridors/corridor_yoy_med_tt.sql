SELECT corridor AS "Corridor", direction AS "Direction", 
	to_char(amq2.tt_med - amq1.tt_med, 'FM90.0') AS "AM Peak Travel Time",
	to_char(amq2.tt_95th - amq1.tt_95th - (amq2.tt_med - amq1.tt_med),'FM90.0') AS "AM Peak Buffer Time",
	to_char(pmq2.tt_med - pmq1.tt_med, 'FM90.0') AS "PM Peak Travel Time",
	to_char(pmq2.tt_95th - pmq1.tt_95th - (pmq2.tt_med - pmq1.tt_med), 'FM90.0') AS "PM Peak Buffer Time"
	
  FROM key_corridor_perf amq1
  INNER JOIN key_corridor_perf amq2 USING (corridor_id, daytype)
  INNER JOIN key_corridor_perf pmq1 USING (corridor_id, daytype)
  INNER JOIN key_corridor_perf pmq2 USING (corridor_id, daytype)
  INNER JOIN key_corridor_lookup USING (corridor_id)
  WHERE daytype = 'Midweek'  
  AND amq1.quarter = '2014-04-01' AND amq1.period = 'AMPK'
  AND amq2.quarter = '2016-04-01' AND amq2.period = 'AMPK'
  AND pmq1.quarter = '2014-04-01' AND pmq1.period = 'PMPK'
  AND pmq2.quarter = '2016-04-01' AND pmq2.period = 'PMPK'
ORDER BY corridor, direction;

