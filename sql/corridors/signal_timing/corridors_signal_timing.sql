DROP TABLE IF EXISTS corridors_signal_timing_lookup;
SELECT DISTINCT ON (roadname, direction) row_number() OVER () AS signal_timing_id, roadname, direction
INTO corridors_signal_timing_lookup
  FROM corridors_signal_timing;

UPDATE corridors_signal_timing t
   SET signal_timing_id = l.signal_timing_id 
   FROM corridors_signal_timing_lookup l
   WHERE t.roadname = l.roadname AND t.direction = l.direction