TRUNCATE 	here_analysis.freq_table;

INSERT INTO 	here_analysis.freq_table (corridor_id, seq_a, seq_b, spd_bin_a, spd_bin_b, freq)
SELECT 		A.corridor_id, A.seq as seq_a, B.seq AS seq_b, ceiling(round(a.spd_avg,1)/5.0) AS spd_bin_a, ceiling(round(b.spd_avg,1)/5.0) AS spd_bin_b, COUNT(*) AS freq
FROM 		here_analysis.corridor_links_15min A
INNER JOIN 	here_analysis.corridor_links_15min B USING (corridor_id, datetime_bin)
WHERE 		A.seq <> B.seq
		AND ABS(A.seq - B.seq) <= 10
		AND A.spd_avg IS NOT NULL AND B.spd_avg IS NOT NULL
		AND A.estimated = FALSE
GROUP BY 	A.corridor_id, A.seq, B.seq, ceiling(round(a.spd_avg,1)/5.0), ceiling(round(b.spd_avg,1)/5.0);