DROP TABLE IF EXISTS here_analysis.corridor_link_agg;

CREATE TABLE here_analysis.corridor_link_agg (
	corridor_id integer not null,
	link_dir text not null,
	date_start date not null,
	date_end date not null,
	hh integer not null,
	tt numeric
	);