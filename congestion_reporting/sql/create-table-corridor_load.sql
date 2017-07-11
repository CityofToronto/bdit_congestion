DROP TABLE IF EXISTS here_analysis.corridor_load;

CREATE TABLE here_analysis.corridor_load (
	corridor_load_id serial primary key not null,
	street character varying(50) not null,
	street_alt character varying(50),
	intersection_start character varying(50) not null,
	intersection_end character varying(50) not null,
	direction character varying(2) not null,
	counter_direction boolean,
	geom geometry(MultiLineString, 32190)
	);