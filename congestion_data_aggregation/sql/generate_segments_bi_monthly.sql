/* 
FUNCTION: generate_segments_bi_monthly
PURPOSE: This function produces estimates of a segment-level buffer index (BI) for every hour of the day, every month
INPUTS: dt_: first day of month
*/

-- DROP FUNCTION congestion.generate_segments_bi_monthly(date);

CREATE OR REPLACE FUNCTION congestion.generate_segments_bi_monthly(
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
    SELECT		segment_id, 
                link_dir,
                date_trunc('hour',tx) AS datetime_bin,
				harmean(mean) AS spd_avg, 
				length AS link_length,
				COUNT (DISTINCT tx)  AS count
    
	FROM 		here.ta
    INNER JOIN  congestion.segment_links_v5_19_4_tc using (link_dir)
	LEFT JOIN   ref.holiday hol ON hol.dt = tx::date
    
    WHERE 		hol.dt IS NULL AND 
                date_part('isodow'::text, tx::date) < 6 AND 
                (tx >= _dt AND tx < ( _dt + '1 mon'::interval))
    
    GROUP BY 	segment_id, link_dir, datetime_bin, link_length),

/*
seg_tt: Produces estimates of the average travel time for each 60-minute bin for each individual segment (segment_id)
*/
seg_tt AS (
    SELECT		segment_id,
                datetime_bin,
				CASE WHEN SUM(link_length) >= 0.8 * b.length 
					THEN SUM(link_length / spd_avg  * 3.6 ) * b.length / SUM(link_length)
				END AS segment_tt_avg
	
    FROM 		speed_links
	INNER JOIN 	congestion.segments_v5 b USING (segment_id)
	
    GROUP BY 	segment_id,  datetime_bin, b.length
	ORDER BY 	segment_id,  datetime_bin
)

    
/*
Final output: Inserts an estimate of the segment buffer index (BI) into congestion.segments_bi_monthly, where at least 80% of the segments (by distance) has observations at the link (link_id) level   
*/
INSERT INTO congestion.segments_bi_monthly
    
SELECT 		a.segment_id,
            date_trunc('month', datetime_bin) AS month,
            datetime_bin::time without time zone AS time_bin,
            count(a.datetime_bin) AS num_bins,
            avg(a.segment_tt_avg) AS avg_tt,
			percentile_cont(0.95::double precision) WITHIN GROUP (ORDER BY a.segment_tt_avg) AS pct_95,
            (percentile_cont(0.95::double precision) WITHIN GROUP (ORDER BY a.segment_tt_avg) - avg(a.segment_tt_avg))/ avg(a.segment_tt_avg) AS bi
    
FROM 		seg_tt a
    
GROUP BY 	a.segment_id,  month, time_bin
ORDER BY 	a.segment_id, month, time_bin


$BODY$;

ALTER FUNCTION congestion.generate_segments_bi_monthly(date)
    OWNER TO congestion_admins;