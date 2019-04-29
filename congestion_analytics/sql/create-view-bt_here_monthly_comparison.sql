WITH initial AS (
	SELECT mth, analysis_id::bigint AS analysis_id, group_id, time_bin, SUM(spd_avg)/ST_Length(ST_Transform(geom, 2019)) FROM
	here_analysis.monthly_averages
	INNER JOIN gis_shared_streets.here_bt_ref_aakash USING (link_dir)
	GROUP BY mth, analysis_id, group_id, time_bin, ST_Length(ST_Transform(geom, 2019))
)

SELECT *
FROM here_analysis.bt_monthly_averages A
INNER JOIN initial USING (analysis_id, mth, group_id, time_bin)
