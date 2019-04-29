DROP MATERIALIZED VIEW here_analysis.analysis_links;

CREATE MATERIALIZED VIEW here_analysis.analysis_links AS 
 
SELECT link_dir, link_id, B.num_bins
FROM here_analysis.agg_30min_201812 A
INNER JOIN here_analysis.link_obs_201812 B ON LEFT(A.link_dir,-1)::numeric = B.link_id
INNER JOIN here_gis.streets_att_18_3 C USING (link_id)
WHERE 	C.func_class NOT IN ('1','2') 
	AND B.num_bins > 446
	AND st_name NOT IN (	'ALLEN RD',
				'DON VALLEY PKWY',
				'GARDINER EXPY',
				'HARBOUR ST',
				'HWY-400',
				'HWY-401 COLLECTORS',
				'HWY-427',
				'HWY-427 COLLECTORS',
				'LAKE SHORE BLVD E',
				'LAKE SHORE BLVD W'
			)
	AND C.ar_pedest != 'N'
GROUP BY link_dir, link_id, B.num_bins
ORDER BY link_dir