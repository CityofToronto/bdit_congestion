DROP TABLE IF EXISTS here_analysis.corridor_load;

CREATE TABLE here_analysis.corridor_load (
	corridor_load_id serial primary key not null,
	geom geometry(MultiLineString, 4326)
	);