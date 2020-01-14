--Returned 210,050,753 rows
--Query returned successfully in 52 min 33 secs.

CREATE MATERIALIZED VIEW congestion.data_fall2019_5min
TABLESPACE pg_default
AS
 SELECT ta.*
   FROM here.ta ta
     LEFT JOIN ref.holiday hol ON hol.dt = ta.tx::date
  WHERE ta.tx >= '2019-09-01 00:00:00'::timestamp without time zone AND ta.tx <= '2019-11-30 23:59:59'::timestamp without time zone AND hol.dt IS NULL AND date_part('isodow'::text, ta.tx::date)::integer < 6
  ORDER BY ta.tx
WITH DATA;

--create index on link_dir (neccessary?)
CREATE INDEX data_fall2019_5min_link_dir
ON congestion.data_fall2019_5min (link_dir)

--ta.* = link_dir, tx, epoch_min, length, mean, stddev, min_spd, max_spd, confidence, 
--pct_5, pct_10, pct_15, pct_20, pct_25, pct_30, pct_35, pct_40, pct_45, pct_50,
--pct_55, pct_60, pct_65, pct_70, pct_75, pct_80, pct_85, pct_90, pct_95