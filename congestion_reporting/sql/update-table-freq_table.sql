DROP TABLE IF EXISTS freq_ranges;

CREATE TEMPORARY TABLE freq_ranges (
	freq_id integer,
	corridor_id integer,
	seq_a smallint,
	seq_b smallint,
	spd_bin_a smallint,
	spd_bin_b smallint,
	low_pct numeric,
	high_pct numeric);

INSERT INTO freq_ranges
SELECT A.freq_id, A.corridor_id, A.seq_a, A.seq_b, A.spd_bin_a, A.spd_bin_b, SUM(CASE WHEN B.spd_bin_b < A.spd_bin_b THEN B.freq ELSE 0 END)*1.0/SUM(B.freq)*1.0 as low_pct, SUM(CASE WHEN B.spd_bin_b <= A.spd_bin_b THEN B.freq ELSE 0 END)*1.0/SUM(B.freq)*1.0 as high_pct
FROM here_analysis.freq_table A
INNER JOIN here_analysis.freq_table B USING (corridor_id, seq_a, seq_b, spd_bin_a)
GROUP BY A.freq_id, A.corridor_id, A.seq_a, A.seq_b, A.spd_bin_a, A.spd_bin_b;

UPDATE here_analysis.freq_table A
SET low_pct = B.low_pct, high_pct = B.high_pct
FROM freq_ranges B
WHERE A.freq_id = B.freq_id;