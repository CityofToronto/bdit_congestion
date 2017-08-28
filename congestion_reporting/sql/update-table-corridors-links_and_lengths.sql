UPDATE here_analysis.corridors A
SET num_links = (SELECT COUNT(*) FROM here_analysis.corridor_links B WHERE B.corridor_id = A.corridor_id);

UPDATE here_analysis.corridors A
SET length_km = (SELECT SUM(distance_km) FROM here_analysis.corridor_links B WHERE B.corridor_id = A.corridor_id);