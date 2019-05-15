INSERT INTO 	here_analysis.agg_30min_trucksonly
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here_analysis.ta_trucks_dec_16_18
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;
