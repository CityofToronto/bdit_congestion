SELECT corridor, direction, amq2.tt_med - amq1.tt_med AS "AM Peak Travel Time Difference", pmq2.tt_med - pmq1.tt_med AS "PM Peak Travel Time Difference"
  FROM key_corridor_perf amq1
  INNER JOIN key_corridor_perf amq2 USING (corridor, direction, daytype)
  INNER JOIN key_corridor_perf pmq1 USING (corridor, direction, daytype)
  INNER JOIN key_corridor_perf pmq2 USING (corridor, direction, daytype)
  WHERE daytype = 'Midweek'  
  AND amq1.quarter = '2014-04-01' AND amq1.period = 'AMPK'
  AND amq2.quarter = '2016-04-01' AND amq2.period = 'AMPK'
  AND pmq1.quarter = '2014-04-01' AND pmq1.period = 'PMPK'
  AND pmq2.quarter = '2016-04-01' AND pmq2.period = 'PMPK'
ORDER BY corridor, direction

  ;
