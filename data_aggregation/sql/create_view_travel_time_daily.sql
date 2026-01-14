CREATE OR REPLACE VIEW congestion.travel_time_daily AS

SELECT
    daily_spd.segment_id,
    daily_spd.dt,
    daily_spd.hr,
    coalesce(current_v.total_length, retired_v.total_length) / daily_spd.spd AS tt,
    daily_spd.is_valid,
    daily_spd.num_bin
FROM congestion.network_segments_daily_spd AS daily_spd
LEFT JOIN congestion.network_segments AS current_v USING (segment_id)
LEFT JOIN
    congestion.network_segments_retired AS retired_v ON daily_spd.segment_id = retired_v.segment_id
AND daily_spd.dt >= retired_v.valid_from
AND daily_spd.dt < retired_v.valid_to;

ALTER TABLE congestion.travel_time_daily OWNER TO congestion_admins;

GRANT SELECT ON TABLE congestion.travel_time_daily TO bdit_humans;
GRANT ALL ON TABLE congestion.travel_time_daily TO congestion_admins;

COMMENT ON VIEW congestion.travel_time_daily
IS 'View that contains aggregated network segments hourly travel time for each day.';