UPDATE here_analysis.corridor_links_15min B
SET tt_avg = A.distance_km / B.spd_avg * 3600.0
FROM here_analysis.corridor_links A
WHERE B.estimated = TRUE AND A.corridor_id = B.corridor_id AND A.seq = B.seq