DROP TABLE IF EXISTS here_analysis.corridor_tt;

CREATE TABLE here_analysis.corridor_tt (
	id serial primary key,
	corridor_id integer not null,
	tx timestamp without time zone not null,
	tt numeric not null
	);