
/* 
FUNCTION: generate_citywide_tti_hourly
PURPOSE: This function produces estimates of a city-wide travel time index (TTI) for every hour of the day, every month
INPUTS: dt_: first day of month
*/

-- DROP FUNCTION congestion.generate_citywide_tti_hourly(date);

CREATE OR REPLACE FUNCTION congestion.generate_citywide_tti_hourly(
	_dt date)
	
    RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE SECURITY DEFINER 

AS $BODY$

/*
speed_links: Produces estimates of the average speed for each 60-minute bin for each individual link (link_dir)
*/
WITH speed_links AS (
	SELECT 		segment_id, 
				link_dir,
				length AS link_length, 
				date_trunc('hour', tx) AS datetime_bin,
				harmean(mean) AS spd_avg,
				COUNT(DISTINCT tx)  AS count_hc
	
	FROM  		here.ta
    INNER JOIN 	congestion.segment_links_v5_19_4 USING (link_dir)
	LEFT JOIN 	ref.holiday hol ON hol.dt = tx::date
	
	WHERE 		hol.dt IS NULL AND 
				date_part('isodow'::text, tx::date) < 6 AND 
				(tx >=  _dt AND tx < (  $1 + '1 mon'::interval))

	GROUP BY 	segment_id, link_dir, datetime_bin, length
	), 
	
/*
hourly_tti: Produces estimates of the average travel time for each 60-minute bin for each individual segment (segment_id), where at least 80% of the segment (by distance) has observations at the link (link_dir) level
*/
hourly_tti AS (
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
monthly: Produces estimates of the average travel time for each month for each hour by segment (segment_id)
*/
monthly AS (
	SELECT		segment_id, 
				date_trunc('month', datetime_bin) as month, 
				datetime_bin::time without time zone as time_bin,
				avg(segment_tt_avg) as segment_tt_avg 
	
	FROM 		hourly_tti

	WHERE 		segment_tt_avg IS NOT NULL 
	
	GROUP BY 	month, time_bin, segment_id
),

/*
seg_tti: Produces estimates of the average travel time index (using 10th percentile travel times for highways and 25th percentile for other segments as a baseline) for each month for each hour by segment (segment_id)
*/
seg_tti AS (
	SELECT 		segment_id, 
				month, 
				time_bin,  
				CASE	WHEN highway.segment_id IS NOT NULL 
							THEN tti.segment_tt_avg/b.tt_baseline_10pct_corr 
						ELSE
							tti.segment_tt_avg/b.tt_baseline_25pct_corr 
				END AS tti
	
	FROM		monthly tti
	LEFT JOIN 	congestion.tt_segments_baseline_v5_2019_af b USING (segment_id)
	LEFT JOIN 	congestion.highway_segments_v5 highway USING (segment_id)
)

/*
Final Output: Inserts an estimate of the city-wide TTI (weighted by length of segment and sqrt of AADT) into congestion.citywide_tti_hourly
*/

INSERT INTO 	congestion.citywide_tti_hourly(month, time_bin, num_segments, tti)
SELECT 			month, 
				time_bin, 
				count(seg_tti.segment_id) AS num_segments,
				sum(seg_tti.tti * segments_v5.length * sqrt(segment_aadt_final.aadt))  / sum(segments_v5.length * sqrt(segment_aadt_final.aadt)) AS tti

FROM			seg_tti 
INNER JOIN 		congestion.segments_v5 USING (segment_id)
INNER JOIN		covid.segment_aadt_final USING (segment_id)

WHERE 			time_bin <@ '[07:00:00, 23:00:00)'::timerange

GROUP BY 		month, time_bin
ORDER BY 		month, time_bin

$BODY$;

ALTER FUNCTION congestion.generate_citywide_tti_hourly(date)
    OWNER TO natalie; -- change this to a group user role
