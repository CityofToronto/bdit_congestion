
INSERT INTO 	here_analysis.agg_30min_hc_201712
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201712
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201711
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201711
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201710
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201710
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201709
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201709
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201708
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201708
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201707
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201707
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;


INSERT INTO 	here_analysis.agg_30min_hc_201706
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201706
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201705
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201705
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201704
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201704
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201703
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201703
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201702
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201702
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201701
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201701
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;





INSERT INTO 	here_analysis.agg_30min_hc_201612
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201612
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201611
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201611
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201610
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201610
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201609
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201609
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201608
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201608
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201607
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201607
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;


INSERT INTO 	here_analysis.agg_30min_hc_201606
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201606
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201605
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201605
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201604
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201604
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201603
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201603
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201602
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201602
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201601
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201601
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;




INSERT INTO 	here_analysis.agg_30min_hc_201512
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201512
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201511
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201511
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201510
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201510
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201509
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201509
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201508
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201508
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;

INSERT INTO 	here_analysis.agg_30min_hc_201507
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201507
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;


INSERT INTO 	here_analysis.agg_30min_hc_201506
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201506
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201505
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201505
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201504
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201504
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201503
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201503
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201502
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201502
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;



INSERT INTO 	here_analysis.agg_30min_hc_201501
SELECT 		link_dir,
		COUNT(1) AS num_bins,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800 AS datetime_bin, 
		1.0/AVG(1.0/pct_50) AS spd_avg

FROM 		here.ta_201501
WHERE		confidence >= 30
GROUP BY 	link_dir,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * floor(extract('epoch' FROM tx) / 1800) * 1800;