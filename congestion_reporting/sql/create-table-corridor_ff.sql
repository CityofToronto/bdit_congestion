DROP TABLE IF EXISTS here_analysis.corridor_ff;

CREATE TABLE here_analysis.corridor_ff (
	corridor_id integer,
	tt_ff numeric);

INSERT INTO here_analysis.corridor_ff(corridor_id, tt_ff)
SELECT corridor_id, SUM(tt_ff) AS tt_night
FROM (SELECT corridor_id, seq, PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY tt_avg) AS tt_ff, COUNT(*) AS OBS
FROM here_analysis.corridor_links_15min
WHERE datetime_bin::time >= '09:00:00' AND datetime_bin::time < '15:00:00'
GROUP BY corridor_id, seq
ORDER BY corridor_id, seq) A
GROUP BY A.corridor_id;