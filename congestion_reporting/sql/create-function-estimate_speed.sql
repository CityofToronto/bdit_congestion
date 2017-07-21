CREATE OR REPLACE FUNCTION here_analysis.estimate_speed(corr_id integer, s_a smallint, s_b smallint, s_bin_a smallint) RETURNS numeric AS $$

DECLARE
-- sample random number WHERE > low_pct and <= high_pct
-- sample random number within 5 kph bin
	rand1 NUMERIC;
	rand2 NUMERIC;
	spd_bin SMALLINT;
	spd NUMERIC;

BEGIN
	rand1 := random();
	rand2 := random();
	spd_bin	:= (SELECT spd_bin_b FROM here_analysis.freq_table WHERE corridor_id = corr_id AND seq_a = s_a AND seq_b = s_b AND spd_bin_a = s_bin_a AND low_pct <= rand1 AND high_pct > rand1);
	spd := rand2*5.0 + (spd_bin-1)*5.0;
	RETURN spd;

END;
$$ LANGUAGE plpgsql;