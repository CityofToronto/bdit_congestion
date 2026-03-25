CREATE OR REPLACE FUNCTION congestion.insert_excluded_px(px_input text, ver_id TEXT)
RETURNS VOID  AS $$
DECLARE
    network_table TEXT;

BEGIN

    network_table := 'temp_network_segments_' || ver_id;

    EXECUTE format(
        'WITH signals AS (
            SELECT DISTINCT 
                a.*,
                case when seg.segment_id is not null then true else false end as in_network
            FROM gis.traffic_signal a
            LEFT JOIN congestion.%I seg
                ON ST_DWITHIN(ST_TRANSFORM(seg.geom, 2952), ST_TRANSFORM(a.geom, 2952), 25
                )
            WHERE a.px = %L
        )
        INSERT INTO congestion.excluded_signals 
        SELECT signals.px, main_street, midblock_route, side1_street, side2_street, private_access, additional_info, x, y, latitude, longitude, activationdate, signalsystem, non_system, control_mode, pedwalkspeed, aps_operation, numberofapproaches, objectid, geo_id, node_id, audiblepedsignal, transit_preempt, fire_preempt, rail_preempt, mi_prinx, geom, bicycle_signal, ups, led_blankout_sign, lpi_north_implementation_date, lpi_south_implementation_date, lpi_east_implementation_date, lpi_west_implementation_date, lpi_comment, aps_activation_date, leading_pedestrian_intervals, removed_date, temp_signal, dist, closest_int, in_network
		FROM signals
		CROSS JOIN LATERAL ( 
		SELECT px,signals.geom as px_geom, intersection_id as closest_int, ints.geom  as int_geom, 
		ST_TRAnsform(signals.geom, 2952)<-> ST_TRAnsform(ints.geom, 2952) as dist
		FROM gis_core.intersection_latest ints 
		ORDER BY ST_TRAnsform(signals.geom, 2952)<-> ST_TRAnsform(ints.geom, 2952)
		limit 1) AS ints ',
        network_table,
        px_input
    );

END;
$$ LANGUAGE plpgsql;
