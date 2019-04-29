CREATE OR REPLACE VIEW here_analysis.bt_monthly_averages AS
SELECT analysis_id, date_trunc('month', datetime_bin) AS mth, C.group_id, C.time_bin, AVG(tt_avg) AS tt_avg
FROM here_analysis.bt_agg_30min A
INNER JOIN here_analysis.dow_group_bins C ON EXTRACT(isodow FROM A.datetime_bin) = C.dow AND A.datetime_bin::time without time zone =  C.time_bin
GROUP BY date_trunc('month', datetime_bin), analysis_id, C.group_id, C.time_bin
ORDER BY date_trunc('month', datetime_bin), analysis_id, C.group_id, C.time_bin;