CREATE VIEW here_analysis.bt_monthly_averages_all AS

SELECT analysis_id, date_trunc('month', datetime_bin) AS mth, B.group_id, B.time_bin, AVG(tt_avg) AS tt_avg
FROM here_analysis.bt_agg_30min_all A
INNER JOIN here_analysis.dow_group_bins B ON EXTRACT(isodow FROM A.datetime_bin) = B.dow AND A.datetime_bin::time without time zone = B.time_bin
GROUP BY date_trunc('month', datetime_bin), analysis_id, B.group_id, B.time_bin;