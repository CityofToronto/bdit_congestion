CREATE OR REPLACE VIEW congestion.least_reliable_annual_50 AS
SELECT row_number() OVER (PARTITION BY agg_period ORDER BY bti DESC) AS "Rank",
 direction AS "Dir", 
 bti as "Buffer Time Index",
 geom,
 agg_period

FROM congestion.metrics 
INNER JOIN congestion.aggregation_levels USING (agg_id)
INNER JOIN gis.inrix_tmc_tor USING (tmc)
WHERE agg_level = 'year'
ORDER BY bti DESC
LIMIT 50