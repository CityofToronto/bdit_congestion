
CREATE OR REPLACE FUNCTION congestion.generate_network_monthly(
	_dt date)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$ 
    
	INSERT INTO congestion.network_segments_monthly
	SELECT 		segment_id, 
				date_trunc('month', dt) AS mth, 
				hr,
				CASE WHEN extract(isodow from dt) <6 then 'Weekday'
					ELSE 'Weekend' END AS day_type,
				round(avg(tt), 2) AS avg_tt,
				PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tt) AS median_tt,
				PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY tt) AS pct_85_tt,
				PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tt) AS pct_95_tt,
				round(min(tt), 2) AS min_tt,
				round(max(tt), 2) AS max_tt,
				stddev(tt) as std_dev,
				sum(num_bins)::int AS num_bins
    
    FROM  		congestion.network_segments_daily
	LEFT JOIN 	ref.holiday USING (dt) -- exclude holidays
    WHERE 		(dt >= _dt AND dt < _dt + INTERVAL '1 month') and 
				holiday.dt IS NULL 
    
	GROUP BY    segment_id, mth, hr, day_type
	ORDER BY    segment_id, mth, hr, day_type

$BODY$;

ALTER FUNCTION congestion.generate_network_monthly(date)
    OWNER TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.generate_network_monthly(date) TO congestion_admins;
GRANT EXECUTE ON FUNCTION congestion.generate_network_monthly(date) TO congestion_bot;
COMMENT ON FUNCTION congestion.generate_network_monthly(date)
    IS 'Function that aggregate network segments hourly travel time and travel time index for each month, excluding holidays. Runs every month through an airflow process.';	