DROP TABLE IF EXISTS simulation_prep;
DROP TABLE IF EXISTS simulation_prep2;

CREATE TEMPORARY TABLE simulation_prep(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_a smallint,
	x integer,
	y integer,
	seq smallint);

CREATE TEMPORARY TABLE simulation_prep2(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_a smallint,
	seq_b smallint,
	spd_bin_b smallint);

INSERT INTO simulation_prep (corridor_id, datetime_bin, seq_a, x, y)
SELECT A.corridor_id, A.datetime_bin as datetime_bin, A.seq AS seq_a, MIN(CASE WHEN A.seq > B.seq THEN A.seq - B.seq ELSE NULL END) as x, MIN(CASE WHEN B.seq > A.seq THEN B.seq - A.seq ELSE NULL END) as y
FROM here_analysis.corridor_links_15min A
INNER JOIN here_analysis.corridor_links_15min B USING (corridor_id, datetime_bin)
WHERE A.estimated = TRUE AND ABS(A.seq - B.seq) <= 10 AND B.estimated = FALSE
GROUP BY A.corridor_id, A.datetime_bin, A.seq;


UPDATE simulation_prep
SET seq = CASE WHEN coalesce(x,99) < coalesce(y,99) THEN seq_a-x ELSE seq_a+y END;

INSERT INTO simulation_prep2(corridor_id, datetime_bin, seq_a, seq_b, spd_bin_b)
SELECT A.corridor_id, A.datetime_bin, A.seq_a, seq AS seq_b, ceiling(round(spd_avg,1)/5.0) AS spd_bin_b
FROM simulation_prep A
INNER JOIN here_analysis.corridor_links_15min B USING (datetime_bin, corridor_id, seq);

SELECT *, here_analysis.estimate_speed(corridor_id,seq_b, seq_a, spd_bin_b) FROM simulation_prep2 LIMIT 1000;

UPDATE 		here_analysis.corridor_links_15min A
SET		avg_spd = here_analysis.estimate_speed(corridor_id,seq_b, seq_a, spd_bin_b)
WHERE 		estimated = TRUE;