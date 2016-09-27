DROP TABLE IF EXISTS ref.timeperiod_ranges;
SELECT period, timerange( min("time"), CASE WHEN period != 'NIGHT' THEN max("time") + interval '15 minutes' ELSE '24:00:00'::TIME END) AS period_range
INTO ref.timeperiod_ranges
  FROM ref.timeperiod
  GROUP BY period
  ;
ALTER TABLE ref.timeperiod_ranges ADD PRIMARY KEY(period)