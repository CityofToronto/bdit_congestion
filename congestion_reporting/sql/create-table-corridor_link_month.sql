DROP TABLE IF EXISTS here_analysis.corridor_link_month;

CREATE TABLE here_analysis.corridor_link_month (
  corridor_id integer NOT NULL,
  link_dir text NOT NULL,
  dt daterange,
  month_bin date,
  day_type integer,
  hh numeric NOT NULL,
  tt_avg numeric,
  tt_med numeric,
  obs integer,
  tt_65 numeric,
  tt_75 numeric,
  tt_85 numeric,
  tt_95 numeric,
  tt_35 numeric,
  tt_25 numeric,
  tt_15 numeric,
  tt_05 numeric
)