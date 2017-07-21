CREATE OR REPLACE FUNCTION here_analysis.closest_valid_link_bin(corr_id integer, seq_b smallint, dt_bin timestamp without time zone) RETURNS smallint AS $$

DECLARE
	seq_low INT;
	seq_high INT;
	dist_low NUMERIC;
	dist_seq NUMERIC;
	dist_high NUMERIC;
	len_low NUMERIC;
	len_seq NUMERIC;
	len_high NUMERIC;
	diff_low NUMERIC;
	diff_high NUMERIC;

BEGIN
	seq_low := (SELECT MAX(seq) FROM here_analysis.corridor_links_15min A WHERE A.datetime_bin = dt_bin AND A.corridor_id = corr_id AND seq < seq_b AND estimated = FALSE AND ABS(seq-seq_b) <= 10);
	seq_high := (SELECT MIN(seq) FROM here_analysis.corridor_links_15min A WHERE A.datetime_bin = dt_bin AND A.corridor_id = corr_id AND seq > seq_b AND estimated = FALSE AND ABS(seq-seq_b) <= 10);
	dist_seq := (SELECT tot_distance_km FROM here_analysis.corridor_links A WHERE A.corridor_id = corr_id AND A.seq = seq_b);
	len_seq := (SELECT distance_km FROM here_analysis.corridor_links A WHERE A.corridor_id = corr_id AND A.seq = seq_b);
	dist_low := (SELECT tot_distance_km FROM here_analysis.corridor_links A WHERE A.corridor_id = corr_id AND A.seq = seq_low);
	len_low := (SELECT distance_km FROM here_analysis.corridor_links A WHERE A.corridor_id = corr_id AND A.seq = seq_low);
	dist_high := (SELECT tot_distance_km FROM here_analysis.corridor_links A WHERE A.corridor_id = corr_id AND A.seq = seq_high);
	len_high := (SELECT distance_km FROM here_analysis.corridor_links A WHERE A.corridor_id = corr_id AND A.seq = seq_high);
	diff_low := ABS((dist_low - len_low/2) - (dist_seq - len_seq/2));
	diff_high := ABS((dist_high - len_high/2) - (dist_seq - len_seq/2));

IF diff_high > diff_low THEN
	RETURN (SELECT ceiling(round(spd_avg,1)/5.0) FROM here_analysis.corridor_links_15min A WHERE A.datetime_bin = dt_bin AND A.corridor_id = corr_id AND seq = seq_low);
ELSE
	RETURN (SELECT ceiling(round(spd_avg,1)/5.0) FROM here_analysis.corridor_links_15min A WHERE A.datetime_bin = dt_bin AND A.corridor_id = corr_id AND seq = seq_high);
END IF;
END;
$$ LANGUAGE plpgsql;