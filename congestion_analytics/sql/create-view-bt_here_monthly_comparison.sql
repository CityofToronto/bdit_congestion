DROP MATERIALIZED VIEW here_analysis.bt_here_monthly_comparison;
CREATE MATERIALIZED VIEW here_analysis.bt_here_monthly_comparison AS
WITH initial AS (
	SELECT mth, analysis_id::bigint AS analysis_id, group_id, time_bin, SUM(ST_Length(ST_Transform(geom, 2019))/spd_avg*3.6) AS tt_here FROM
	here_analysis.monthly_averages
	INNER JOIN gis_shared_streets.here_bt_ref_aakash USING (link_dir)
	GROUP BY mth, analysis_id::bigint, group_id, time_bin
)

SELECT analysis_id, mth, group_id, time_bin, tt_here, A.tt_avg AS tt_bt
FROM here_analysis.bt_monthly_averages A
INNER JOIN initial USING (analysis_id, mth, group_id, time_bin)
