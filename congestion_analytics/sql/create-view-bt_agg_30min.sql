CREATE MATERIALIZED VIEW here_analysis.bt_agg_30min AS 
SELECT 
analysis_id,
TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM datetime_bin) / 1800) * 1800 AS datetime_bin,
SUM(obs) AS num_obs,
avg(tt) AS tt_avg
FROM bluetooth.aggr_5min
WHERE analysis_id IN (1453284, 1453464, 1455510, 1455555)
GROUP BY analysis_id, TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM datetime_bin) / 1800) * 1800
ORDER BY analysis_id, TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM datetime_bin) / 1800) * 1800