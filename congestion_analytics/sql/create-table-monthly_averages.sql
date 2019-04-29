CREATE TABLE here_analysis.monthly_averages (
	link_dir text,
	mth date,
	group_id smallint,
	time_bin time without time zone,
	spd_avg numeric
);

INSERT INTO here_analysis.monthly_averages
SELECT link_dir, date_trunc('month', datetime_bin) AS mth, C.group_id, C.time_bin, 1.0/AVG(1.0/spd_avg) AS spd_avg
FROM here_analysis.agg_30min A
INNER JOIN here_analysis.analysis_links B USING (link_dir)
INNER JOIN here_analysis.dow_group_bins C ON EXTRACT(isodow FROM A.datetime_bin) = C.dow AND A.datetime_bin::time without time zone =  C.time_bin
GROUP BY date_trunc('month', datetime_bin), link_dir, C.group_id, C.time_bin;