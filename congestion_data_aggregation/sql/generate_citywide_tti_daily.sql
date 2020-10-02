/* 
FUNCTION: generate_citywide_tti_daily
PURPOSE: This function produces estimates of a city-wide travel time index (TTI) for every 30-minute of the day
INPUTS: dt_: day of aggregation
*/

-- DROP FUNCTION congestion.generate_citywide_tti_daily(date);

CREATE OR REPLACE FUNCTION congestion.generate_citywide_tti_daily(
	_dt date)
	
    RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE SECURITY DEFINER 

AS $BODY$

/*
speed_links: Produces estimates of the average speed for each 30-minute bin for each individual link (link_dir)
*/
WITH speed_links AS (
	SELECT 		segment_id, 
				link_dir,
				length AS link_length, 
				(TIMESTAMP WITHOUT TIME ZONE 'epoch' +
                    INTERVAL '1 second' * (floor((extract('epoch' from tx)) / 1800) * 1800))::time AS datetime_bin,
				harmean(mean) AS spd_avg,
				COUNT(DISTINCT tx)  AS count
	
	FROM  		here.ta
    INNER JOIN 	congestion.segment_links_v5_19_4_tc USING (link_dir)
	
	WHERE 	    (tx >=  _dt AND tx < (  $1 + '1 day'::interval))

	GROUP BY 	segment_id, link_dir, datetime_bin, length
), 
	
/*
daily: Produces estimates of the average travel time for each 30-minute bin for each individual segment (segment_id), where at least 80% of the segment (by distance) has observations at the link (link_dir) level
*/
daily AS (
	SELECT 		segment_id, 
				datetime_bin, 
				CASE 	WHEN SUM(link_length) >= 0.8 * b.length 
							THEN SUM(link_length / spd_avg  * 3.6 ) * b.length / SUM(link_length)
						ELSE
							NULL 
				END AS segment_tt_avg 

	FROM 		speed_links
	INNER JOIN 	congestion.segments_v5 b USING (segment_id)

	WHERE 		link_length / spd_avg IS NOT NULL

	GROUP BY 	segment_id, datetime_bin, b.length
	ORDER BY 	segment_id, datetime_bin
),

/*
seg_tti: Produces estimates of the average travel time index (using 10th percentile travel times for highways and 25th percentile for other segments as a baseline) for each month for each hour by segment (segment_id)
*/
seg_tti AS (
	SELECT 		segment_id, 
				datetime_bin::date AS dt, 
				datetime_bin::time without time zone AS time_bin,  
				CASE	WHEN highway.segment_id IS NOT NULL 
							THEN tti.segment_tt_avg/b.tt_baseline_10pct_corr 
						ELSE
							tti.segment_tt_avg/b.tt_baseline_25pct_corr 
				END AS tti
	
	FROM		daily tti
	LEFT JOIN 	congestion.tt_segments_baseline_v5_2019_af b USING (segment_id)
	LEFT JOIN 	congestion.highway_segments_v5 highway USING (segment_id)
)

/*
Final Output: Inserts an estimate of the city-wide TTI (weighted by length of segment and sqrt of AADT) into congestion.citywide_tti_daily
*/

INSERT INTO 	congestion.citywide_tti_daily(dt, time_bin, num_segments, tti)
SELECT 			dt, 
				time_bin, 
				count(seg_tti.segment_id) AS num_segments,
				sum(seg_tti.tti * segments_v5.length * sqrt(segment_aadt_final.aadt))  / sum(segments_v5.length * sqrt(segment_aadt_final.aadt)) AS tti

FROM			seg_tti 
INNER JOIN 		congestion.segments_v5 USING (segment_id)
INNER JOIN		covid.segment_aadt_final USING (segment_id)

WHERE 		    time_bin <@ '[06:00:00, 23:00:00]'::timerange

GROUP BY 		dt, time_bin
ORDER BY 		dt, time_bin


$BODY$;

ALTER FUNCTION congestion.generate_citywide_tti_daily(date)
    OWNER TO congestion_admins;
