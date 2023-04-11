CREATE TABLE congestion.tt_corridor_30min_v4_2019 AS

WITH X AS
(
SELECT corridor_id, a.segment_id, a.link_dir, a.datetime_bin, a.link_length, 
a.spd_avg_all, a.spd_avg_hc,
a.spd_med_all, a.spd_med_hc,
a.link_length / a.spd_avg_all  * 3.6 AS link_tt_avg_all,
a.link_length / a.spd_avg_hc  * 3.6 AS link_tt_avg_hc,
a.link_length / a.spd_med_all  * 3.6 AS link_tt_med_all,
a.link_length / a.spd_med_hc  * 3.6 AS link_tt_med_hc,
b.length AS cor_length
FROM congestion.speeds_links_30min_v4_2019 a
INNER JOIN congestion.corridor_segments_v4 b 
USING (segment_id)
GROUP BY corridor_id, segment_id, a.link_dir, datetime_bin, link_length, spd_avg_all, spd_avg_hc, spd_med_all, spd_med_hc, b.length
ORDER BY corridor_id, segment_id, a.link_dir
)

, Y AS ( --all=all confidence level 
SELECT corridor_id,  datetime_bin, 
CASE WHEN SUM(link_length) >= 0.8 * cor_length 
THEN SUM(link_tt_avg_all) * cor_length / SUM(link_length)
END AS corridor_tt_avg_all ,
CASE WHEN SUM(link_length) >= 0.8 * cor_length 
THEN SUM(link_tt_med_all) * cor_length / SUM(link_length)
END AS corridor_tt_med_all ,
SUM(link_length) / cor_length * 100 AS data_pct_all
FROM X
GROUP BY corridor_id, datetime_bin, cor_length
ORDER BY corridor_id, datetime_bin
)

, Z AS ( --hc=high confidence >= 30
SELECT corridor_id, datetime_bin, 
CASE WHEN SUM(link_length) >= 0.8 * cor_length 
THEN SUM(link_tt_avg_hc) * cor_length / SUM(link_length)
END AS corridor_tt_avg_hc ,
CASE WHEN SUM(link_length) >= 0.8 * cor_length 
THEN SUM(link_tt_med_hc) * cor_length / SUM(link_length)
END AS corridor_tt_med_hc ,
SUM(link_length) / cor_length * 100 AS data_pct_hc
FROM X
WHERE link_tt_avg_hc IS NOT NULL
AND link_tt_med_hc IS NOT NULL
GROUP BY corridor_id, datetime_bin, cor_length
ORDER BY corridor_id, datetime_bin
)

SELECT Y.corridor_id, Y.datetime_bin, 
corridor_tt_avg_all, corridor_tt_med_all, data_pct_all,
corridor_tt_avg_hc, corridor_tt_med_hc, data_pct_hc
FROM Y
LEFT JOIN Z
USING (corridor_id, datetime_bin)