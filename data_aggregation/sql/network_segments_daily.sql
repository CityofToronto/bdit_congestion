-- Parent table structure
CREATE TABLE IF NOT EXISTS congestion.network_segments_daily
(
    segment_id integer NOT NULL,
    dt date NOT NULL,
    hr numeric NOT NULL,
    tt numeric NOT NULL,
    unadjusted_tt numeric NOT NULL,
    length_w_data numeric NOT NULL,
    is_valid boolean NOT NULL,
    num_bins integer NOT NULL
) PARTITION BY RANGE (dt);

ALTER TABLE congestion.network_segments_daily OWNER TO congestion_admins;
GRANT SELECT ON TABLE congestion.network_segments_daily TO bdit_humans;

COMMENT ON TABLE congestion.network_segments_daily
IS 'Table that contains network segments hourly travel time for each day. Partition monthly using dt column. New daily data automatically aggregated through an airflow process.';
