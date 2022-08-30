CREATE OR REPLACE FUNCTION congestion.generate_network_weekly(
	_dt date)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$ 
    
	INSERT INTO congestion.network_segments_weekly
	SELECT 		segment_id, 
				_dt as week, 
				hr,
				CASE WHEN extract(isodow from dt) <6 then 'Weekday'
					ELSE 'Weekend' END AS day_type,
				round(avg(tt),2) AS tt,
				round(avg(tti),2) AS tti,
				sum(num_bins)::int AS num_bins
    
    FROM  		congestion.network_segments_daily
	LEFT JOIN 	ref.holiday USING (dt)
    WHERE 		(dt >= _dt AND dt < _dt + INTERVAL '1 week') and 
				holiday.dt IS NULL 
    
	GROUP BY    segment_id, week, hr, day_type
	ORDER BY    segment_id, week, hr, day_type

$BODY$;

ALTER FUNCTION congestion.generate_network_weekly(date)
    OWNER TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.generate_network_weekly(date) TO congestion_admins;
GRANT EXECUTE ON FUNCTION congestion.generate_network_weekly(date) TO congestion_bot;
COMMENT ON FUNCTION congestion.generate_network_weekly(date)
    IS 'Function that aggregate network segments hourly travel time and travel time index for each week, excluding holidays. Runs every week through an airflow process.';	