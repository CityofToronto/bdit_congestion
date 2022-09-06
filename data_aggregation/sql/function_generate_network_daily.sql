CREATE OR REPLACE FUNCTION congestion.generate_network_daily(
	_dt date)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

WITH speed_links AS (
    SELECT 		segment_id, 
				link_dir,
				links.length AS link_length,
				dt, 
				extract(hour from tod)::int AS hr,
				harmean(mean) AS spd_avg,
				COUNT(tx)::int as num_bin
    
    FROM  		here.ta
    INNER JOIN 	congestion.network_links_22_2 links USING (link_dir)
    WHERE 		(dt >= _dt AND dt < _dt + interval '1 day') 
    
	GROUP BY    segment_id, link_dir, dt, hr, links.length
),

/*
tt_hr: Produces estimates of the average travel time for each 1 hour bin for each individual segment (segment_id), 
		where at least 80% of the segment (by distance) has observations at the link (link_dir) level
*/
tt_hr AS (
    SELECT 		segment_id, 
                dt,
				hr,
                CASE WHEN SUM(link_length) >= 0.8 * total_length 
                          THEN SUM(link_length / spd_avg  * 3.6 ) * total_length / SUM(link_length)
                     ELSE 
                         NULL 
                END AS segment_avg_tt,
				CASE WHEN highway IS false 
						THEN baseline_10pct
					ELSE
						baseline_25pct
				END AS baseline_tt,
				sum(num_bin) as num_bin
    
    FROM 		speed_links
    INNER JOIN 	congestion.network_segments USING (segment_id)
    LEFT JOIN 	congestion.network_baseline USING (segment_id)
	
    GROUP BY	segment_id, dt, hr, total_length, highway, baseline_10pct, baseline_25pct
    ORDER BY	segment_id, dt, hr
)

/*
Final Output: Inserts an estimate of the segment aggregation into congestion.network_segments_daily
*/
INSERT INTO     congestion.network_segments_daily      
SELECT 			segment_id,
                dt,
                hr,
                round(segment_avg_tt::numeric, 2) as tt,
				num_bin
FROM 			tt_hr
WHERE 			segment_avg_tt IS NOT NULL

$BODY$;

ALTER FUNCTION congestion.generate_network_daily(date)
    OWNER TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.generate_network_daily(date) TO congestion_admins;
GRANT EXECUTE ON FUNCTION congestion.generate_network_daily(date) TO congestion_bot;
COMMENT ON FUNCTION congestion.generate_network_daily(date)
    IS 'Function that aggregate network segments hourly travel time and travel time index for each day. Runs everyday through an airflow process.';