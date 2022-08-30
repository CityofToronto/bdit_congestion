-- Parent table structure
CREATE TABLE IF NOT EXISTS congestion.network_segments_monthly
(
    segment_id integer NOT NULL,
	mth date NOT NULL,
	hr integer NOT NULL,
	day_type text,
	tt numeric NOT NULL, 
    tti numeric NOT NULL,
	num_bins integer  NOT NULL
) PARTITION BY RANGE (mth);

ALTER TABLE congestion.network_segments_monthly OWNER TO congestion_admins;
GRANT SELECT ON TABLE congestion.network_segments_monthly TO bdit_humans;

COMMENT ON TABLE congestion.network_segments_monthly
    IS 'Table that contains network segments hourly travel time and travel time index for each monthly. Partition yearly using mth column. New monthly data automatically aggregated through an airflow process.';
