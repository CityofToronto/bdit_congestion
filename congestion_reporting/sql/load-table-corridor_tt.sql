INSERT INTO here_analysis.corridor_tt (corridor_id, tx, tt)

SELECT C.corridor_id, A.tx, SUM(pct_50) as tt
FROM here.ta A
INNER JOIN here_analysis.corridor_links B USING (link_dir)
INNER JOIN here_analysis.corridors C USING (corridor_id)
WHERE A.tx >= '2016-01-01'
GROUP BY C.corridor_id, A.tx