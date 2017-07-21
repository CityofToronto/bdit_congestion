DROP TABLE IF EXISTS excluded_bins;

CREATE TEMPORARY TABLE excluded_bins (
		corridor_id integer,
		datetime_bin timestamp without time zone
		);

INSERT INTO 	excluded_bins
SELECT		A.corridor_id,
		A.datetime_bin
FROM		here_analysis.corridor_links_15min A
INNER JOIN	here_analysis.corridors B USING (corridor_id)
GROUP BY	A.corridor_id, A.datetime_bin, B.num_links
HAVING		COUNT(A.link_dir) < 0.6*B.num_links;


UPDATE 		here_analysis.corridor_links_15min A
SET		excluded = TRUE
FROM		excluded_bins B
WHERE 		A.corridor_id = B.corridor_id
		AND A.datetime_bin = B.datetime_bin;