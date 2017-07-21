DROP TABLE IF EXISTS here_analysis.corridor_links_15min;

CREATE TABLE here_analysis.corridor_links_15min (
	corridor_id integer,
	link_dir text,
	datetime_bin timestamp without time zone,
	seq integer,
	spd_avg numeric,
	tt_avg numeric,
	obs smallint,
	excluded boolean,
	estimated boolean);