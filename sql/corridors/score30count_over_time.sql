SELECT roadname, date_trunc('Month', dt)::DATE as "Month", time_15_continuous/10 as hh, SUM(count) as obs, SUM(count)/COUNT(DISTINCT(tmc))as "Observations per Segment"
FROM rdumas.agg_extract_hour_subsample 
INNER JOIN rdumas.arterial_corridors USING (tmc)
GROUP BY roadname, "Month", hh
ORDER BY roadname, "Month", hh