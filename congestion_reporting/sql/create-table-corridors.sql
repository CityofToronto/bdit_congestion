DROP TABLE IF EXISTS here_analysis.corridors;

CREATE TABLE here_analysis.corridors (
	corridor_id serial primary key,
	corridor_name varchar(100) not null,
	length_km numeric not null,
	street varchar(50),
	direction varchar(10),
	group_id integer,
	group_order integer
	);