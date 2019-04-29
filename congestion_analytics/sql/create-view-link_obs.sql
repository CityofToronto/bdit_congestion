DROP MATERIALIZED VIEW here_analysis.link_obs_201812;

CREATE MATERIALIZED VIEW here_analysis.link_obs_201812 AS 
 
WITH link_dir_obs AS (
SELECT agg_30min_201812.link_dir AS link_dir,
    count(1) AS bins
   FROM here_analysis.agg_30min_201812
  GROUP BY agg_30min_201812.link_dir
)

SELECT 	LEFT(link_dir, -1)::numeric AS link_id,
	AVG(bins) AS num_bins
FROM 	link_dir_obs
GROUP BY LEFT(link_dir,-1)::numeric
ORDER BY AVG(bins) desc
WITH DATA;