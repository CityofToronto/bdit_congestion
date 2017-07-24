DROP TABLE IF EXISTS here_analysis.corridor_summary;

CREATE TABLE here_analysis.corridor_summary (
	summary_id serial,
	corridor_id integer,
	year_bin date,
	month_bin date,
	hh smallint,
	tt_avg numeric,
	tt_50 numeric,
	tt_95 numeric,
	tti numeric,
	bti numeric);