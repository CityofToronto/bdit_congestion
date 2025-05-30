CREATE TABLE IF NOT EXISTS congestion.network_segments_daily_spd
(
    segment_id bigint,
    dt date,
    hr integer,
    spd numeric,
    length_w_data numeric,
    total_length numeric,
    is_valid boolean,
    num_bin bigint
);

ALTER TABLE IF EXISTS congestion.network_segments_daily_spd OWNER TO congestion_admins;
GRANT SELECT ON TABLE congestion.network_segments_daily_spd TO bdit_humans; -- maybe they should only have access to travel time?

COMMENT ON TABLE congestion.network_segments_daily_spd
IS 'Table that contains aggregated network segments hourly speed for each day.';