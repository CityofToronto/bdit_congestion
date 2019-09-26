DROP TABLE IF EXISTS here_analysis.corridors;

CREATE TABLE here_analysis.corridors (
	corridor_id serial primary key,
	corridor_name varchar(100) not null,
	length_km numeric,
	street varchar(50),
	direction varchar(10),
	intersection_start character varying(50),
	intersection_end character varying(50),
	group_id integer,
	group_order integer,
	num_links integer,
	corridor_load_id integer
	);