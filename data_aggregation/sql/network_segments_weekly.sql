CREATE TABLE IF NOT EXISTS congestion.network_segments_weekly
(
    segment_id integer NOT NULL,
	week date NOT NULL,
	hr numeric,
	day_type text,
	tt numeric NOT NULL, 
    tti numeric NOT NULL,
	num_bins integer  NOT NULL
);

ALTER TABLE congestion.network_segments_weekly OWNER TO congestion_admins;
GRANT SELECT ON TABLE congestion.network_segments_weekly TO bdit_humans;

COMMENT ON TABLE congestion.network_segments_weekly
    IS 'Table that contains network segments hourly travel time and travel time index for each week. New weekly data automatically aggregated through an airflow process.';
