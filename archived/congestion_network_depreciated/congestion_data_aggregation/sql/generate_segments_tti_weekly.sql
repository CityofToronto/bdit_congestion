/* 
FUNCTION: generate_segments_tti_weekly
PURPOSE: This function produces estimates of a segment-level travel time index (TTI) for every 30-minutes of the day, every week
INPUTS: _dt: first day of the week
*/


-- DROP FUNCTION congestion.generate_segments_tti_weekly(date);

-- FUNCTION: congestion.generate_segments_tti_weekly(date)

CREATE OR REPLACE FUNCTION congestion.generate_segments_tti_weekly(
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
                    INTERVAL '1 second' * (floor((extract('epoch' from tx)) / 1800) * 1800)) AS datetime_bin,
				harmean(mean) AS spd_avg,
				COUNT (DISTINCT tx)  AS count
    
    FROM  		here.ta
    INNER JOIN 	congestion.segment_links_v5_19_4_tc USING (link_dir)
	LEFT JOIN 	ref.holiday hol ON hol.dt = tx::date
    
    WHERE 		hol.dt IS NULL AND 
			    (tx < _dt AND tx >= (  _dt - '1 week'::interval))
    
	GROUP BY    segment_id, link_dir, datetime_bin, length
),

/*
tt_30: Produces estimates of the average travel time for each 30-minute bin for each individual segment (segment_id), where at least 80% of the segment (by distance) has observations at the link (link_dir) level
*/
tt_30 AS (
    SELECT 		segment_id, 
                datetime_bin, 
				CASE WHEN date_part('isodow', datetime_bin)::int <@ '[1,6)'::int4range
                          THEN 'Weekday'
                     ELSE
                        'Weekend'
                END as day_type,
                CASE WHEN SUM(link_length) >= 0.8 * b.length 
                          THEN SUM(link_length / spd_avg  * 3.6 ) * b.length / SUM(link_length)
                     ELSE 
                         NULL 
                END AS segment_tt_avg 
    
    FROM 		speed_links
    INNER JOIN 	congestion.segments_v5 b USING (segment_id)
    
    GROUP BY	segment_id, datetime_bin, b.length, day_type
    ORDER BY	segment_id, datetime_bin
),

/*
weekly_tt: Produces estimates of the average travel time for each week for each 30-minute bin by segment (segment_id)
*/
weekly_tt AS (
    SELECT 		a.segment_id,
            	a.datetime_bin::time without time zone AS time_bin,
            	count(a.datetime_bin) AS num_bins,
            	date_trunc('week'::text, a.datetime_bin) AS week,
                day_type, 
            	b.seg_length,
            	avg(a.segment_tt_avg) AS avg_tt,
                CASE
                    WHEN highway.segment_id IS NOT NULL 
                         THEN b.tt_baseline_10pct_corr
                    ELSE 
                         b.tt_baseline_25pct_corr
                    END AS baseline_tt
    
    FROM	 	tt_30 a
    LEFT JOIN 	congestion.tt_segments_baseline_v5_2019_af b USING (segment_id)
    LEFT JOIN 	congestion.highway_segments_v5  highway USING (segment_id)
    
    GROUP BY 	a.segment_id, week, highway.segment_id, time_bin, day_type,
				b.seg_length, b.tt_baseline_10pct_corr, b.tt_baseline_25pct_corr
    ORDER BY    a.segment_id, week, time_bin
)

/*
Final Output: Inserts an estimate of the segment TTI into congestion.segments_tti_weekly
*/
INSERT INTO     congestion.segments_tti_weekly       
SELECT 			tti.segment_id,
                tti.week,
                tti.day_type,
                tti.time_bin,
                tti.num_bins AS tti_num_bins,
                tti.avg_tt,
                tti.avg_tt / tti.baseline_tt AS tti
FROM 			weekly_tt tti
   

$BODY$;

ALTER FUNCTION congestion.generate_segments_tti_weekly(date)
    OWNER TO congestion_admins;
