
CREATE OR REPLACE FUNCTION congestion.generate_centreline_monthly(
	_dt date)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$ 
    
	INSERT INTO congestion.centreline_monthly

    WITH centreline_daily AS (
        
	SELECT  	uid, 
                dt, 
                hr, 
				((segment_set_length / (sum(total_length) / sum(tt))) * cent_length)/segment_set_length as tt,
                baseline_10pct, 
                baseline_25pct
	
    FROM        congestion.network_segments_daily
	INNER JOIN  congestion.segment_centreline_lookup using (segment_id)
	INNER JOIN  congestion.network_segments using (segment_id)
    LEFT JOIN   congestion.centreline_baseline using (uid)
	LEFT JOIN 	ref.holiday USING (dt) -- exclude holidays
	WHERE       (dt >= _dt AND dt < _dt + INTERVAL '1 month') AND 
				yr::int = EXTRACT(YEAR From _dt) AND
				holiday.dt IS NULL 
        
	GROUP BY 	uid, dt, hr, total_length, baseline_10pct, baseline_25pct, segment_set_length, cent_length
	HAVING 		sum(total_length) >= (segment_set_length * 0.8))
    
    SELECT 		uid, 
				date_trunc('month', a.dt) AS mth, 
				hr,
				CASE WHEN extract(isodow from dt) <6 then 'Weekday'
					ELSE 'Weekend' END AS day_type,
				round(avg(tt), 2) AS avg_tt,
				PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tt) AS median_tt,
				PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY tt) AS pct_85_tt,
				PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tt) AS pct_95_tt,
				round(min(tt), 2) AS min_tt,
				round(max(tt), 2) AS max_tt,
				stddev(tt) AS std_dev,
                baseline_10pct AS py_pct_10_tt,
                baseline_25pct AS py_pct_25_tt
    
    FROM  		centreline_daily a
	LEFT JOIN 	ref.holiday USING (dt) -- exclude holidays
    WHERE 		holiday.dt IS NULL 
    
	GROUP BY    uid, mth, hr, day_type, baseline_10pct, baseline_25pct
	ORDER BY    uid, mth, hr, day_type
	

$BODY$;

ALTER FUNCTION congestion.generate_centreline_monthly(date)
    OWNER TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.generate_centreline_monthly(date) TO congestion_admins;
GRANT EXECUTE ON FUNCTION congestion.generate_centreline_monthly(date) TO congestion_bot;
COMMENT ON FUNCTION congestion.generate_centreline_monthly(date)
    IS 'Function that aggregate centreline equivalent of network segments hourly travel time for each month, excluding holidays. Runs every month through an airflow process.';	