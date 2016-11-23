CREATE OR REPLACE VIEW congestion.least_reliable_annual AS
SELECT row_number() OVER (PARTITION BY agg_period ORDER BY bti DESC) AS "Rank",
 street_name AS "Street",
 direction AS "Dir", 
 from_to AS "From - To",
 bti as "Buffer Time Index", 
 agg_period AS "Year",
 geom,
 gid

FROM congestion.metrics 
INNER JOIN congestion.aggregation_levels USING (agg_id)
INNER JOIN gis.inrix_tmc_tor USING (tmc)
INNER JOIN gis.tmc_from_to_lookup USING (tmc)
WHERE agg_level = 'year'
ORDER BY bti DESC