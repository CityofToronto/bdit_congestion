DROP TABLE IF EXISTS here_analysis.freq_table;

CREATE TABLE here_analysis.freq_table (
	freq_id serial not null,
	corridor_id integer,
	seq_a smallint,
	seq_b smallint,
	spd_bin_a smallint,
	spd_bin_b smallint,
	freq integer,
	low_pct numeric,
	high_pct numeric);