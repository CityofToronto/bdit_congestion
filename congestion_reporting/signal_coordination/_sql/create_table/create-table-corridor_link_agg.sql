DROP TABLE IF EXISTS here_analysis.corridor_link_agg;

CREATE TABLE here_analysis.corridor_link_agg (
	corridor_id integer not null,
	link_dir text not null,
	dt daterange,
	day_type integer,
	hh numeric not null,
	tt_avg numeric,
	tt_med numeric,
	obs integer
	);