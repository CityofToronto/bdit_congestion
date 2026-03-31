-- FUNCTION: congestion.check_for_new_intersections_px()

-- DROP FUNCTION IF EXISTS congestion.check_for_new_intersections_px();

CREATE OR REPLACE FUNCTION congestion.check_for_new_intersections_px(
	)
    RETURNS TABLE(px text, old_int integer, new_int integer, old_dist double precision, new_dist double precision) 
    LANGUAGE 'sql'
    COST 100
    STABLE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
with possible_signals as (
SELECT * FROM congestion.excluded_signals
where in_network = true and removed_date is null and temp_signal is null)

SELECT  ints.px, 
		signals.closest_int as old_int, intersection_id as new_int, 
		signals.dist as old_dist, distance as new_dist 
FROM possible_signals signals
CROSS JOIN LATERAL ( 
SELECT px,signals.geom as px_geom, intersection_id, ints.geom  as int_geom, 
ST_TRAnsform(signals.geom, 2952)<-> ST_TRAnsform(ints.geom, 2952) as distance
FROM gis_core.intersection_latest ints 
ORDER BY ST_TRAnsform(signals.geom, 2952)<-> ST_TRAnsform(ints.geom, 2952)
limit 1) AS ints
WHERE signals.closest_int != intersection_id
$BODY$;

ALTER FUNCTION congestion.check_for_new_intersections_px()
    OWNER TO congestion_admins;

