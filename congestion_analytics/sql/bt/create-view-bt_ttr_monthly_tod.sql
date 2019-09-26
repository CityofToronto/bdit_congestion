CREATE OR REPLACE VIEW here_analysis.bt_ttr_monthly_tod AS 
SELECT 		mth,
		group_id,
		time_bin,
		sum(ST_Length(ST_Transform(d.geom,98012))*b.tt_avg / a.tt_avg)/SUM(ST_Length(ST_Transform(d.geom,98012)))
FROM 		(	SELECT group_id, time_bin, analysis_id, tt_avg
			FROM here_analysis.bt_monthly_averages
			WHERE mth = '2017-10-01'
		) a
			
INNER JOIN	here_analysis.bt_monthly_averages b USING (group_id, time_bin, analysis_id)
INNER JOIN 	king_pilot.bt_segments c USING (analysis_id)
INNER JOIN	bluetooth.segments d USING (analysis_id)
WHERE		analysis_id <> ALL (ARRAY[1453535::bigint, 1453806::bigint, 1454832::bigint, 1454853::bigint, 1453239::bigint, 1453507::bigint, 1454127::bigint, 1454449::bigint, 1454181::bigint, 1454196::bigint, 1454209::bigint, 1454224::bigint, 1454340::bigint, 1454352::bigint, 1454366::bigint, 1454378::bigint])
GROUP BY 	mth, group_id, time_bin
ORDER BY 	mth, group_id, time_bin