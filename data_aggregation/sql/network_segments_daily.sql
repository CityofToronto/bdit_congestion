-- Parent table structure
CREATE TABLE IF NOT EXISTS congestion.network_segments_daily
(
    segment_id text NOT NULL,
	dt date NOT NULL,
	hr numeric,
	tt numeric NOT NULL, 
    tti numeric NOT NULL,
	num_bins integer  NOT NULL
) PARTITION BY RANGE (dt);

ALTER TABLE congestion.network_segments_daily OWNER TO congestion_admins;
GRANT SELECT ON TABLE congestion.network_segments_daily TO bdit_humans;

COMMENT ON TABLE congestion.network_segments_daily
    IS 'Table that contains network segments hourly travel time and travel time index for each day. Partition yearly using dt column. New daily data automatically aggregated through an airflow process.';
