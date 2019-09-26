CREATE OR REPLACE VIEW here_analysis.bt_ttr_monthly AS
SELECT DISTINCT A.mth, A.period_id, SUM(B.length * A.ttr) / SUM(B.length) AS ttr_all
FROM here_analysis.bt_ttr_period_links A
INNER JOIN bluetooth.segments B USING (analysis_id)
INNER JOIN king_pilot.bt_segments C USING (analysis_id)
WHERE period_id IN (1,2,3,4,5) AND analysis_id <> ALL (ARRAY[1453535::bigint, 1453806::bigint, 1454832::bigint, 1454853::bigint, 1453239::bigint, 1453507::bigint, 1454127::bigint, 1454449::bigint, 1454181::bigint, 1454196::bigint, 1454209::bigint, 1454224::bigint, 1454340::bigint, 1454352::bigint, 1454366::bigint, 1454378::bigint])
GROUP BY a.mth, A.period_id
ORDER BY  a.period_id, a.mth
