DROP TABLE IF EXISTS	all_links;
DROP TABLE IF EXISTS	all_link_bins;


CREATE TEMPORARY TABLE	all_links (
				corridor_id integer,
				link_dir text,
				seq smallint
			);


CREATE TEMPORARY TABLE	all_link_bins (
				corridor_id integer,
				datetime_bin timestamp without time zone
			);


INSERT INTO 		all_links
SELECT DISTINCT		corridor_id,
			link_dir,
			seq
FROM			here_analysis.corridor_links_15min
ORDER BY		corridor_id, link_dir, seq;


INSERT INTO		all_link_bins
SELECT DISTINCT		corridor_id,
			datetime_bin
FROM			here_analysis.corridor_links_15min
WHERE			excluded = FALSE AND EXTRACT(hour FROM datetime_bin) IN (8,17)
ORDER BY		corridor_id, datetime_bin;


INSERT INTO		here_analysis.corridor_links_15min (corridor_id, link_dir, datetime_bin, seq, excluded, estimated)
SELECT			B.corridor_id,
			B.link_dir,
			A.datetime_bin,
			B.seq,
			FALSE as excluded,
			TRUE as estimated
FROM			all_link_bins A
INNER JOIN		all_links B USING (corridor_id)
LEFT JOIN		here_analysis.corridor_links_15min C USING (corridor_id, seq, datetime_bin)
WHERE			C.tt_avg IS NULL
ORDER BY		B.corridor_id, B.seq, A.datetime_bin;
			