/*
-- UPDATE here_analysis.corridor_links_15min
-- SET spd_avg = NULL, tt_avg = NULL
-- WHERE estimated = TRUE;

-- ROUND ONE
DROP TABLE IF EXISTS simulation_prep;
DROP TABLE IF EXISTS simulation_prep2;
DROP TABLE IF EXISTS simulation_prep3;

CREATE TEMPORARY TABLE simulation_prep(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_b smallint,
	x integer,
	y integer,
	seq smallint);

CREATE TEMPORARY TABLE simulation_prep2(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_a smallint,
	seq_b smallint,
	spd_bin_a smallint,
	dist_rand numeric);
	
CREATE TEMPORARY TABLE simulation_prep3(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq smallint,
	avg_spd numeric);
	
INSERT INTO simulation_prep (corridor_id, datetime_bin, seq_b, x, y)
SELECT A.corridor_id, A.datetime_bin as datetime_bin, A.seq AS seq_b, MIN(CASE WHEN A.seq > B.seq THEN A.seq - B.seq ELSE NULL END) as x, MIN(CASE WHEN B.seq > A.seq THEN B.seq - A.seq ELSE NULL END) as y
FROM here_analysis.corridor_links_15min A
INNER JOIN here_analysis.corridor_links_15min B USING (corridor_id, datetime_bin)
WHERE A.estimated = TRUE AND ABS(A.seq - B.seq) <= 10 AND B.estimated = FALSE
GROUP BY A.corridor_id, A.datetime_bin, A.seq;

UPDATE simulation_prep
SET seq = CASE WHEN coalesce(x,99) < coalesce(y,99) THEN seq_b-x ELSE seq_b+y END;

INSERT INTO 	simulation_prep2(corridor_id, datetime_bin, seq_a, seq_b, spd_bin_a, dist_rand)
SELECT 		A.corridor_id, A.datetime_bin, seq AS seq_a, A.seq_b, ceiling(round(spd_avg,1)/5.0) AS spd_bin_a, random() AS dist_rand
FROM 		simulation_prep A
INNER JOIN 	here_analysis.corridor_links_15min B USING (datetime_bin, corridor_id, seq);


INSERT INTO 	simulation_prep3(corridor_id, datetime_bin, seq, avg_spd)
SELECT 		A.corridor_id, A.datetime_bin, seq_b AS seq, CASE WHEN B.spd_bin_b = 1 THEN 1 + random()*2 + (CASE WHEN random() > 0.333 THEN 2 ELSE 0 END) ELSE (random()*5.0 + ((B.spd_bin_b-1)*5.0)) END as avg_spd
FROM 		simulation_prep2 A
INNER JOIN 	here_analysis.freq_table B USING (corridor_id, seq_a, seq_b, spd_bin_a)
WHERE		low_pct <= dist_rand AND high_pct > dist_rand;


UPDATE 		here_analysis.corridor_links_15min A
SET		spd_avg = B.avg_spd
FROM		simulation_prep3 B
WHERE 		B.corridor_id = A.corridor_id AND A.seq = B.seq AND A.datetime_bin = B.datetime_bin;


-- ROUND TWO

DROP TABLE IF EXISTS simulation_prep;
DROP TABLE IF EXISTS simulation_prep2;
DROP TABLE IF EXISTS simulation_prep3;

CREATE TEMPORARY TABLE simulation_prep(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_b smallint,
	x integer,
	y integer,
	seq smallint);

CREATE TEMPORARY TABLE simulation_prep2(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_a smallint,
	seq_b smallint,
	spd_bin_a smallint,
	dist_rand numeric);
	
CREATE TEMPORARY TABLE simulation_prep3(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq smallint,
	avg_spd numeric);
	
INSERT INTO simulation_prep (corridor_id, datetime_bin, seq_b, x, y)
SELECT A.corridor_id, A.datetime_bin as datetime_bin, A.seq AS seq_b, MIN(CASE WHEN A.seq > B.seq THEN A.seq - B.seq ELSE NULL END) as x, MIN(CASE WHEN B.seq > A.seq THEN B.seq - A.seq ELSE NULL END) as y
FROM here_analysis.corridor_links_15min A
INNER JOIN here_analysis.corridor_links_15min B USING (corridor_id, datetime_bin)
WHERE A.estimated = TRUE AND A.spd_avg IS NULL AND ABS(A.seq - B.seq) <= 10 AND B.spd_avg IS NOT NULL
GROUP BY A.corridor_id, A.datetime_bin, A.seq;

UPDATE simulation_prep
SET seq = CASE WHEN coalesce(x,99) < coalesce(y,99) THEN seq_b-x ELSE seq_b+y END;

INSERT INTO 	simulation_prep2(corridor_id, datetime_bin, seq_a, seq_b, spd_bin_a, dist_rand)
SELECT 		A.corridor_id, A.datetime_bin, seq AS seq_a, A.seq_b, ceiling(round(spd_avg,1)/5.0) AS spd_bin_a, random() AS dist_rand
FROM 		simulation_prep A
INNER JOIN 	here_analysis.corridor_links_15min B USING (datetime_bin, corridor_id, seq);


INSERT INTO 	simulation_prep3(corridor_id, datetime_bin, seq, avg_spd)
SELECT 		A.corridor_id, A.datetime_bin, seq_b AS seq, CASE WHEN B.spd_bin_b = 1 THEN 1 + random()*2 + (CASE WHEN random() > 0.333 THEN 2 ELSE 0 END) ELSE (random()*5.0 + ((B.spd_bin_b-1)*5.0)) END as avg_spd
FROM 		simulation_prep2 A
INNER JOIN 	here_analysis.freq_table B USING (corridor_id, seq_a, seq_b, spd_bin_a)
WHERE		low_pct <= dist_rand AND high_pct > dist_rand;


UPDATE 		here_analysis.corridor_links_15min A
SET		spd_avg = B.avg_spd
FROM		simulation_prep3 B
WHERE 		B.corridor_id = A.corridor_id AND A.seq = B.seq AND A.datetime_bin = B.datetime_bin;


-- ROUND THREE

DROP TABLE IF EXISTS simulation_prep;
DROP TABLE IF EXISTS simulation_prep2;
DROP TABLE IF EXISTS simulation_prep3;

CREATE TEMPORARY TABLE simulation_prep(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_b smallint,
	x integer,
	y integer,
	seq smallint);

CREATE TEMPORARY TABLE simulation_prep2(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_a smallint,
	seq_b smallint,
	spd_bin_a smallint,
	dist_rand numeric);
	
CREATE TEMPORARY TABLE simulation_prep3(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq smallint,
	avg_spd numeric);
	
INSERT INTO simulation_prep (corridor_id, datetime_bin, seq_b, x, y)
SELECT A.corridor_id, A.datetime_bin as datetime_bin, A.seq AS seq_b, MIN(CASE WHEN A.seq > B.seq THEN A.seq - B.seq ELSE NULL END) as x, MIN(CASE WHEN B.seq > A.seq THEN B.seq - A.seq ELSE NULL END) as y
FROM here_analysis.corridor_links_15min A
INNER JOIN here_analysis.corridor_links_15min B USING (corridor_id, datetime_bin)
WHERE A.estimated = TRUE AND A.spd_avg IS NULL AND ABS(A.seq - B.seq) <= 10 AND B.spd_avg IS NOT NULL
GROUP BY A.corridor_id, A.datetime_bin, A.seq;

UPDATE simulation_prep
SET seq = CASE WHEN coalesce(x,99) <= coalesce(y,99) THEN seq_b-x ELSE seq_b+y END;

INSERT INTO 	simulation_prep2(corridor_id, datetime_bin, seq_a, seq_b, spd_bin_a, dist_rand)
SELECT 		A.corridor_id, A.datetime_bin, seq AS seq_a, A.seq_b, ceiling(round(spd_avg,1)/5.0) AS spd_bin_a, random() AS dist_rand
FROM 		simulation_prep A
INNER JOIN 	here_analysis.corridor_links_15min B USING (datetime_bin, corridor_id, seq);


INSERT INTO 	simulation_prep3(corridor_id, datetime_bin, seq, avg_spd)
SELECT 		A.corridor_id, A.datetime_bin, seq_b AS seq, CASE WHEN B.spd_bin_b = 1 THEN 1 + random()*2 + (CASE WHEN random() > 0.333 THEN 2 ELSE 0 END) ELSE (random()*5.0 + ((B.spd_bin_b-1)*5.0)) END as avg_spd
FROM 		simulation_prep2 A
INNER JOIN 	here_analysis.freq_table B USING (corridor_id, seq_a, seq_b, spd_bin_a)
WHERE		low_pct <= dist_rand AND high_pct > dist_rand;


UPDATE 		here_analysis.corridor_links_15min A
SET		spd_avg = B.avg_spd
FROM		simulation_prep3 B
WHERE 		B.corridor_id = A.corridor_id AND A.seq = B.seq AND A.datetime_bin = B.datetime_bin;


-- ROUND FOUR

DROP TABLE IF EXISTS simulation_prep;
DROP TABLE IF EXISTS simulation_prep2;
DROP TABLE IF EXISTS simulation_prep3;

CREATE TEMPORARY TABLE simulation_prep(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_b smallint,
	x integer,
	y integer,
	seq smallint);

CREATE TEMPORARY TABLE simulation_prep2(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_a smallint,
	seq_b smallint,
	spd_bin_a smallint,
	dist_rand numeric);
	
CREATE TEMPORARY TABLE simulation_prep3(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq smallint,
	avg_spd numeric);
	
INSERT INTO simulation_prep (corridor_id, datetime_bin, seq_b, x, y)
SELECT A.corridor_id, A.datetime_bin as datetime_bin, A.seq AS seq_b, MIN(CASE WHEN A.seq > B.seq THEN A.seq - B.seq ELSE NULL END) as x, MIN(CASE WHEN B.seq > A.seq THEN B.seq - A.seq ELSE NULL END) as y
FROM here_analysis.corridor_links_15min A
INNER JOIN here_analysis.corridor_links_15min B USING (corridor_id, datetime_bin)
WHERE A.estimated = TRUE AND A.spd_avg IS NULL AND ABS(A.seq - B.seq) <= 10 AND B.spd_avg IS NOT NULL
GROUP BY A.corridor_id, A.datetime_bin, A.seq;

UPDATE simulation_prep
SET seq = CASE WHEN coalesce(x,99) < coalesce(y,99) THEN seq_b-x-1 ELSE seq_b+y+1 END;

INSERT INTO 	simulation_prep2(corridor_id, datetime_bin, seq_a, seq_b, spd_bin_a, dist_rand)
SELECT 		A.corridor_id, A.datetime_bin, seq AS seq_a, A.seq_b, ceiling(round(spd_avg,1)/5.0) AS spd_bin_a, random() AS dist_rand
FROM 		simulation_prep A
INNER JOIN 	here_analysis.corridor_links_15min B USING (datetime_bin, corridor_id, seq);


INSERT INTO 	simulation_prep3(corridor_id, datetime_bin, seq, avg_spd)
SELECT 		A.corridor_id, A.datetime_bin, seq_b AS seq, CASE WHEN B.spd_bin_b = 1 THEN 1 + random()*2 + (CASE WHEN random() > 0.333 THEN 2 ELSE 0 END) ELSE (random()*5.0 + ((B.spd_bin_b-1)*5.0)) END as avg_spd
FROM 		simulation_prep2 A
INNER JOIN 	here_analysis.freq_table B USING (corridor_id, seq_a, seq_b, spd_bin_a)
WHERE		low_pct <= dist_rand AND high_pct > dist_rand;


UPDATE 		here_analysis.corridor_links_15min A
SET		spd_avg = B.avg_spd
FROM		simulation_prep3 B
WHERE 		B.corridor_id = A.corridor_id AND A.seq = B.seq AND A.datetime_bin = B.datetime_bin;


-- ROUND FIVE

DROP TABLE IF EXISTS simulation_prep;
DROP TABLE IF EXISTS simulation_prep2;
DROP TABLE IF EXISTS simulation_prep3;

CREATE TEMPORARY TABLE simulation_prep(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_b smallint,
	x integer,
	y integer,
	seq smallint);

CREATE TEMPORARY TABLE simulation_prep2(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_a smallint,
	seq_b smallint,
	spd_bin_a smallint,
	dist_rand numeric);
	
CREATE TEMPORARY TABLE simulation_prep3(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq smallint,
	avg_spd numeric);
	
INSERT INTO simulation_prep (corridor_id, datetime_bin, seq_b, x, y)
SELECT A.corridor_id, A.datetime_bin as datetime_bin, A.seq AS seq_b, MIN(CASE WHEN A.seq > B.seq THEN A.seq - B.seq ELSE NULL END) as x, MIN(CASE WHEN B.seq > A.seq THEN B.seq - A.seq ELSE NULL END) as y
FROM here_analysis.corridor_links_15min A
INNER JOIN here_analysis.corridor_links_15min B USING (corridor_id, datetime_bin)
WHERE A.estimated = TRUE AND A.spd_avg IS NULL AND ABS(A.seq - B.seq) <= 10 AND B.spd_avg IS NOT NULL
GROUP BY A.corridor_id, A.datetime_bin, A.seq;

UPDATE simulation_prep
SET seq = CASE WHEN coalesce(x,99) <= coalesce(y,99) THEN seq_b-x-1 ELSE seq_b+y+1 END;

INSERT INTO 	simulation_prep2(corridor_id, datetime_bin, seq_a, seq_b, spd_bin_a, dist_rand)
SELECT 		A.corridor_id, A.datetime_bin, seq AS seq_a, A.seq_b, ceiling(round(spd_avg,1)/5.0) AS spd_bin_a, random() AS dist_rand
FROM 		simulation_prep A
INNER JOIN 	here_analysis.corridor_links_15min B USING (datetime_bin, corridor_id, seq);


INSERT INTO 	simulation_prep3(corridor_id, datetime_bin, seq, avg_spd)
SELECT 		A.corridor_id, A.datetime_bin, seq_b AS seq, CASE WHEN B.spd_bin_b = 1 THEN 1 + random()*2 + (CASE WHEN random() > 0.333 THEN 2 ELSE 0 END) ELSE (random()*5.0 + ((B.spd_bin_b-1)*5.0)) END as avg_spd
FROM 		simulation_prep2 A
INNER JOIN 	here_analysis.freq_table B USING (corridor_id, seq_a, seq_b, spd_bin_a)
WHERE		low_pct <= dist_rand AND high_pct > dist_rand;


UPDATE 		here_analysis.corridor_links_15min A
SET		spd_avg = B.avg_spd
FROM		simulation_prep3 B
WHERE 		B.corridor_id = A.corridor_id AND A.seq = B.seq AND A.datetime_bin = B.datetime_bin;

*/
-- ROUND SIX

DROP TABLE IF EXISTS simulation_prep;
DROP TABLE IF EXISTS simulation_prep2;
DROP TABLE IF EXISTS simulation_prep3;

CREATE TEMPORARY TABLE simulation_prep(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_b smallint,
	x integer,
	y integer,
	seq smallint);

CREATE TEMPORARY TABLE simulation_prep2(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_a smallint,
	seq_b smallint,
	spd_bin_a smallint,
	dist_rand numeric);
	
CREATE TEMPORARY TABLE simulation_prep3(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq smallint,
	avg_spd numeric);
	
INSERT INTO simulation_prep (corridor_id, datetime_bin, seq_b, x, y)
SELECT A.corridor_id, A.datetime_bin as datetime_bin, A.seq AS seq_b, MIN(CASE WHEN A.seq > B.seq THEN A.seq - B.seq ELSE NULL END) as x, MIN(CASE WHEN B.seq > A.seq THEN B.seq - A.seq ELSE NULL END) as y
FROM here_analysis.corridor_links_15min A
INNER JOIN here_analysis.corridor_links_15min B USING (corridor_id, datetime_bin)
WHERE A.estimated = TRUE AND A.spd_avg IS NULL AND ABS(A.seq - B.seq) <= 10 AND B.spd_avg IS NOT NULL
GROUP BY A.corridor_id, A.datetime_bin, A.seq;

UPDATE simulation_prep
SET seq = CASE WHEN coalesce(x,99) < coalesce(y,99) THEN seq_b-10 ELSE seq_b+10 END;

INSERT INTO 	simulation_prep2(corridor_id, datetime_bin, seq_a, seq_b, spd_bin_a, dist_rand)
SELECT 		A.corridor_id, A.datetime_bin, seq AS seq_a, A.seq_b, ceiling(round(spd_avg,1)/5.0) AS spd_bin_a, random() AS dist_rand
FROM 		simulation_prep A
INNER JOIN 	here_analysis.corridor_links_15min B USING (datetime_bin, corridor_id, seq);


INSERT INTO 	simulation_prep3(corridor_id, datetime_bin, seq, avg_spd)
SELECT 		A.corridor_id, A.datetime_bin, seq_b AS seq, CASE WHEN B.spd_bin_b = 1 THEN 1 + random()*2 + (CASE WHEN random() > 0.333 THEN 2 ELSE 0 END) ELSE (random()*5.0 + ((B.spd_bin_b-1)*5.0)) END as avg_spd
FROM 		simulation_prep2 A
INNER JOIN 	here_analysis.freq_table B USING (corridor_id, seq_a, seq_b, spd_bin_a)
WHERE		low_pct <= dist_rand AND high_pct > dist_rand;


UPDATE 		here_analysis.corridor_links_15min A
SET		spd_avg = B.avg_spd
FROM		simulation_prep3 B
WHERE 		B.corridor_id = A.corridor_id AND A.seq = B.seq AND A.datetime_bin = B.datetime_bin;


-- ROUND SEVEN

DROP TABLE IF EXISTS simulation_prep;
DROP TABLE IF EXISTS simulation_prep2;
DROP TABLE IF EXISTS simulation_prep3;

CREATE TEMPORARY TABLE simulation_prep(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_b smallint,
	x integer,
	y integer,
	seq smallint);

CREATE TEMPORARY TABLE simulation_prep2(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq_a smallint,
	seq_b smallint,
	spd_bin_a smallint,
	dist_rand numeric);
	
CREATE TEMPORARY TABLE simulation_prep3(
	corridor_id integer,
	datetime_bin timestamp without time zone,
	seq smallint,
	avg_spd numeric);
	
INSERT INTO simulation_prep (corridor_id, datetime_bin, seq_b, x, y)
SELECT A.corridor_id, A.datetime_bin as datetime_bin, A.seq AS seq_b, MIN(CASE WHEN A.seq > B.seq THEN A.seq - B.seq ELSE NULL END) as x, MIN(CASE WHEN B.seq > A.seq THEN B.seq - A.seq ELSE NULL END) as y
FROM here_analysis.corridor_links_15min A
INNER JOIN here_analysis.corridor_links_15min B USING (corridor_id, datetime_bin)
WHERE A.estimated = TRUE AND A.spd_avg IS NULL AND ABS(A.seq - B.seq) <= 10 AND B.spd_avg IS NOT NULL
GROUP BY A.corridor_id, A.datetime_bin, A.seq;

UPDATE simulation_prep
SET seq = CASE WHEN coalesce(x,99) <= coalesce(y,99) THEN seq_b-10 ELSE seq_b+10 END;

INSERT INTO 	simulation_prep2(corridor_id, datetime_bin, seq_a, seq_b, spd_bin_a, dist_rand)
SELECT 		A.corridor_id, A.datetime_bin, seq AS seq_a, A.seq_b, ceiling(round(spd_avg,1)/5.0) AS spd_bin_a, random() AS dist_rand
FROM 		simulation_prep A
INNER JOIN 	here_analysis.corridor_links_15min B USING (datetime_bin, corridor_id, seq);


INSERT INTO 	simulation_prep3(corridor_id, datetime_bin, seq, avg_spd)
SELECT 		A.corridor_id, A.datetime_bin, seq_b AS seq, CASE WHEN B.spd_bin_b = 1 THEN 1 + random()*2 + (CASE WHEN random() > 0.333 THEN 2 ELSE 0 END) ELSE (random()*5.0 + ((B.spd_bin_b-1)*5.0)) END as avg_spd
FROM 		simulation_prep2 A
INNER JOIN 	here_analysis.freq_table B USING (corridor_id, seq_a, seq_b, spd_bin_a)
WHERE		low_pct <= dist_rand AND high_pct > dist_rand;


UPDATE 		here_analysis.corridor_links_15min A
SET		spd_avg = B.avg_spd
FROM		simulation_prep3 B
WHERE 		B.corridor_id = A.corridor_id AND A.seq = B.seq AND A.datetime_bin = B.datetime_bin;
