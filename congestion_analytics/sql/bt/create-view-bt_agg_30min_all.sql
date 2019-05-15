DROP MATERIALIZED VIEW here_analysis.bt_agg_30min CASCADE;
CREATE MATERIALIZED VIEW here_analysis.bt_agg_30min AS 
SELECT 	analysis_id,
	TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM datetime_bin) / 1800) * 1800 AS datetime_bin,
	SUM(obs) AS num_obs,
	avg(tt) AS tt_avg
FROM	bluetooth.aggr_5min
INNER JOIN bluetooth.segments USING (analysis_id)
WHERE tt > 0.0
GROUP BY analysis_id, TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM datetime_bin) / 1800) * 1800
ORDER BY analysis_id, TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM datetime_bin) / 1800) * 1800;