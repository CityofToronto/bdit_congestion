CREATE OR REPLACE VIEW here_analysis.bt_ttr_monthly_tod AS 
SELECT 		mth,
		group_id,
		time_bin,
		sum(ST_Length(ST_Transform(d.geom,98012))*b.tt_avg / a.tt_avg)/SUM(ST_Length(ST_Transform(d.geom,98012)))
FROM 		(	SELECT group_id, time_bin, analysis_id, tt_avg
			FROM here_analysis.bt_monthly_averages_all
			WHERE mth = '2017-10-01'
		) a
			
INNER JOIN	here_analysis.bt_monthly_averages_all b USING (group_id, time_bin, analysis_id)
INNER JOIN 	king_pilot.bt_segments c USING (analysis_id)
INNER JOIN	bluetooth.segments d USING (analysis_id)
WHERE		street_name != 'King'
GROUP BY 	mth, group_id, time_bin
ORDER BY 	mth, group_id, time_bin