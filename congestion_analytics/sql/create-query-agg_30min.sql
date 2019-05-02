INSERT INTO 	here_analysis.agg_30min_201903
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201903
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_201902
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201902
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_201901
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201901
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;








INSERT INTO 	here_analysis.agg_30min_201412
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201412
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_201411
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201411
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_201410
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201410
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_201409
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201409
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_201408
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201408
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_201407
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201407
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;


INSERT INTO 	here_analysis.agg_30min_201406
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201406
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_201405
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201405
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_201404
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201404
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_201403
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201403
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_201402
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201402
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_201401
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201401
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;