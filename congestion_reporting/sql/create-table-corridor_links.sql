﻿DROP TABLE IF EXISTS here_analysis.corridor_links;

CREATE TABLE here_analysis.corridors_links (
	corridor_link_id serial primary key,
	corridor_id integer not null,
	link_dir text not null,
	seq integer not null,
	distance_km numeric not null
	);