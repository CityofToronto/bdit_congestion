/* 
FUNCTION: generate_corridor_bi_hourly
PURPOSE: This function produces estimates of a corridor-level buffer index (BI) for every hour of the day, every month
INPUTS: dt_: first day of month
*/

-- DROP FUNCTION congestion.generate_corridor_bi_hourly(date);

CREATE OR REPLACE FUNCTION congestion.generate_corridor_bi_hourly(
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
                datetime_bin(tx,60) AS datetime_bin,
				harmean(mean) AS spd_avg_all, 
				length AS link_length,
				COUNT (DISTINCT tx)  AS count_hc
    
	FROM 		here.ta
    INNER JOIN  congestion.segment_links_v5_19_4 using (link_dir)
	LEFT JOIN   ref.holiday hol ON hol.dt = tx::date
    
    WHERE 		hol.dt IS NULL AND 
                date_part('isodow'::text, tx::date) < 6 AND 
                (tx >= _dt AND tx < ( _dt + '1 mon'::interval))
    
    GROUP BY 	segment_id, link_dir, datetime_bin(tx,60), link_length
),

/*
seg_tt: Produces estimates of the average travel time for each 60-minute bin for each individual segment (segment_id)
*/
seg_tt AS (
    SELECT		segment_id,
                datetime_bin,
				CASE WHEN SUM(link_length) >= 0.8 * b.length 
					THEN SUM(link_length / spd_avg_all  * 3.6 ) * b.length / SUM(link_length)
				END AS spd_avg_all,	
				SUM(link_length) / b.length * 100 AS data_pct_hc
	
    FROM 		speed_links
	INNER JOIN 	congestion.segments_v5 b USING (segment_id)
	
    WHERE 		link_length / spd_avg_all  IS NOT NULL
	
    GROUP BY 	segment_id,  datetime_bin, b.length
	ORDER BY 	segment_id,  datetime_bin
),

/*
seg_bi: Produces estimates of the average buffer index for each month for each hour by segment (segment_id)
*/
seg_bi AS (
    SELECT 		a.segment_id,
            	date_trunc('month', datetime_bin) AS month,
            	datetime_bin::time without time zone AS time_bin,
            	count(a.datetime_bin) AS num_bins,
            	avg(a.spd_avg_all) AS avg_tt,
				percentile_cont(0.95::double precision) WITHIN GROUP (ORDER BY a.spd_avg_all) AS pct_95,
            	(percentile_cont(0.95::double precision) WITHIN GROUP (ORDER BY a.spd_avg_all) - avg(a.spd_avg_all))/ avg(a.spd_avg_all) AS bi
    
    FROM 		seg_tt a
    
    WHERE 		a.spd_avg_all IS NOT NULL
    
    GROUP BY 	a.segment_id, (a.datetime_bin::time without time zone), date_trunc('month', datetime_bin)
    ORDER BY 	a.segment_id, date_trunc('month', datetime_bin), (a.datetime_bin::time without time zone)
),

/*
cor_bi: Produces estimates of the buffer index (BI) for each month for each hour by corridor (corridor_id)
*/
cor_bi AS (
	SELECT 		corridor_id, 
                month, 
                time_bin, 
                sum(bi*seg.length)/cor.length AS bi, 
                sum(seg.length) AS seg_length, 
                cor.length AS cor_length
    
	FROM 		seg_bi
	JOIN 		congestion.segments_v5 seg using (segment_id)
	JOIN 		congestion.corridors_v1_merged_lookup using (segment_id)
	JOIN 		congestion.corridors_v1_merged cor using (corridor_id)
    
	GROUP BY corridor_id, month, time_bin, cor.length
)

/*
Final Output: Inserts an estimate of the corridor buffer index (BI) into congestion.corridor_bi_hourly, where at least 80% of the corridor (by distance) has observations at the segment (segment_id) level
*/
INSERT INTO congestion.corridor_bi_hourly

SELECT 			corridor_id, 
                month, 
                time_bin, 
                CASE WHEN cor_length*0.8 < seg_length 
                         THEN bi 
                    ELSE 
                         NULL 
                END AS bi
                
FROM 			cor_bi 

WHERE 			time_bin <@ '[07:00:00, 23:00:00)'::timerange

GROUP BY 		corridor_id, month, time_bin, cor_length, seg_length, bi
ORDER BY 		corridor_id, month, time_bin

$BODY$;

ALTER FUNCTION congestion.generate_corridor_bi_hourly(date)
    OWNER TO congestion_admins;
