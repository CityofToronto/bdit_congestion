CREATE INDEX raw_2014
  ON raw_data_allyears
  USING btree
  (dateandtime)
  WHERE date_part('year'::text, dateandtime) = 2014::double precision AND date_part('month'::text, dateandtime) = 12::double precision;