DROP TABLE IF EXISTS here_analysis.corridor_flags;

CREATE TABLE here_analysis.corridor_flags (
	corridor_id integer,
	ampk_bti_flag smallint,
	pmpk_bti_flag smallint,
	ampk_tti_flag smallint,
	pmpk_tti_flag smallint,
	ampk_spd_flag smallint,
	pmpk_spd_flag smallint);

INSERT INTO here_analysis.corridor_flags(corridor_id)
SELECT DISTINCT corridor_id FROM here_analysis.corridor_summary ORDER BY corridor_id;

UPDATE here_analysis.corridor_flags A
SET ampk_bti_flag = ( CASE 	WHEN B.bti > (SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY bti) FROM here_analysis.corridor_summary WHERE hh = 8 AND year_bin IS NOT NULL) THEN 2
				WHEN B.bti > (SELECT PERCENTILE_CONT(0.65) WITHIN GROUP (ORDER BY bti) FROM here_analysis.corridor_summary WHERE hh = 8 AND year_bin IS NOT NULL) THEN 1
				WHEN B.bti < (SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY bti) FROM here_analysis.corridor_summary WHERE hh = 8 AND year_bin IS NOT NULL) THEN -2
				WHEN B.bti < (SELECT PERCENTILE_CONT(0.35) WITHIN GROUP (ORDER BY bti) FROM here_analysis.corridor_summary WHERE hh = 8 AND year_bin IS NOT NULL) THEN -1
				ELSE 0
				END)
FROM here_analysis.corridor_summary B
WHERE B.hh = 8 AND B.year_bin IS NOT NULL AND B.corridor_id = A.corridor_id;

UPDATE here_analysis.corridor_flags A
SET pmpk_bti_flag = ( CASE 	WHEN B.bti > (SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY bti) FROM here_analysis.corridor_summary WHERE hh = 17 AND year_bin IS NOT NULL) THEN 2
				WHEN B.bti > (SELECT PERCENTILE_CONT(0.65) WITHIN GROUP (ORDER BY bti) FROM here_analysis.corridor_summary WHERE hh = 17 AND year_bin IS NOT NULL) THEN 1
				WHEN B.bti < (SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY bti) FROM here_analysis.corridor_summary WHERE hh = 17 AND year_bin IS NOT NULL) THEN -2
				WHEN B.bti < (SELECT PERCENTILE_CONT(0.35) WITHIN GROUP (ORDER BY bti) FROM here_analysis.corridor_summary WHERE hh = 17 AND year_bin IS NOT NULL) THEN -1
				ELSE 0
				END)
FROM here_analysis.corridor_summary B
WHERE B.hh = 17 AND B.year_bin IS NOT NULL AND B.corridor_id = A.corridor_id;

UPDATE here_analysis.corridor_flags A
SET ampk_tti_flag = ( CASE 	WHEN B.tti > (SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY tti) FROM here_analysis.corridor_summary WHERE hh = 8 AND year_bin IS NOT NULL) THEN 2
				WHEN B.tti > (SELECT PERCENTILE_CONT(0.65) WITHIN GROUP (ORDER BY tti) FROM here_analysis.corridor_summary WHERE hh = 8 AND year_bin IS NOT NULL) THEN 1
				WHEN B.tti < (SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY tti) FROM here_analysis.corridor_summary WHERE hh = 8 AND year_bin IS NOT NULL) THEN -2
				WHEN B.tti < (SELECT PERCENTILE_CONT(0.35) WITHIN GROUP (ORDER BY tti) FROM here_analysis.corridor_summary WHERE hh = 8 AND year_bin IS NOT NULL) THEN -1
				ELSE 0
				END)
FROM here_analysis.corridor_summary B
WHERE B.hh = 8 AND B.year_bin IS NOT NULL AND B.corridor_id = A.corridor_id;

UPDATE here_analysis.corridor_flags A
SET pmpk_tti_flag = ( CASE 	WHEN B.tti > (SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY tti) FROM here_analysis.corridor_summary WHERE hh = 17 AND year_bin IS NOT NULL) THEN 2
				WHEN B.tti > (SELECT PERCENTILE_CONT(0.65) WITHIN GROUP (ORDER BY tti) FROM here_analysis.corridor_summary WHERE hh = 17 AND year_bin IS NOT NULL) THEN 1
				WHEN B.tti < (SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY tti) FROM here_analysis.corridor_summary WHERE hh = 17 AND year_bin IS NOT NULL) THEN -2
				WHEN B.tti < (SELECT PERCENTILE_CONT(0.35) WITHIN GROUP (ORDER BY tti) FROM here_analysis.corridor_summary WHERE hh = 17 AND year_bin IS NOT NULL) THEN -1
				ELSE 0
				END)
FROM here_analysis.corridor_summary B
WHERE B.hh = 17 AND B.year_bin IS NOT NULL AND B.corridor_id = A.corridor_id;

UPDATE here_analysis.corridor_flags A
SET ampk_spd_flag = ( CASE 	WHEN B.spd_avg > (SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY spd_avg) FROM here_analysis.corridor_summary WHERE hh = 8 AND year_bin IS NOT NULL) THEN -2
				WHEN B.spd_avg > (SELECT PERCENTILE_CONT(0.65) WITHIN GROUP (ORDER BY spd_avg) FROM here_analysis.corridor_summary WHERE hh = 8 AND year_bin IS NOT NULL) THEN -1
				WHEN B.spd_avg < (SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY spd_avg) FROM here_analysis.corridor_summary WHERE hh = 8 AND year_bin IS NOT NULL) THEN 2
				WHEN B.spd_avg < (SELECT PERCENTILE_CONT(0.35) WITHIN GROUP (ORDER BY spd_avg) FROM here_analysis.corridor_summary WHERE hh = 8 AND year_bin IS NOT NULL) THEN 1
				ELSE 0
				END)
FROM here_analysis.corridor_summary B
WHERE B.hh = 8 AND B.year_bin IS NOT NULL AND B.corridor_id = A.corridor_id;

UPDATE here_analysis.corridor_flags A
SET pmpk_spd_flag = ( CASE 	WHEN B.spd_avg > (SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY spd_avg) FROM here_analysis.corridor_summary WHERE hh = 17 AND year_bin IS NOT NULL) THEN -2
				WHEN B.spd_avg > (SELECT PERCENTILE_CONT(0.65) WITHIN GROUP (ORDER BY spd_avg) FROM here_analysis.corridor_summary WHERE hh = 17 AND year_bin IS NOT NULL) THEN -1
				WHEN B.spd_avg < (SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY spd_avg) FROM here_analysis.corridor_summary WHERE hh = 17 AND year_bin IS NOT NULL) THEN 2
				WHEN B.spd_avg < (SELECT PERCENTILE_CONT(0.35) WITHIN GROUP (ORDER BY spd_avg) FROM here_analysis.corridor_summary WHERE hh = 17 AND year_bin IS NOT NULL) THEN 1
				ELSE 0
				END)
FROM here_analysis.corridor_summary B
WHERE B.hh = 17 AND B.year_bin IS NOT NULL AND B.corridor_id = A.corridor_id;