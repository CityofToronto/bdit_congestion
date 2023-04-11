/* 
VIEW: congestion.corridor_tti_weekly
PURPOSE: This view contains estimates of a corridor-level travel time index (TTI) for every 30 minute of the day, every week
*/

CREATE OR REPLACE VIEW congestion.corridor_tti_weekly AS 

/*
cor_tti: Produces estimates of the travel time index (TTI) for each week for each hour by corridor (corridor_id)
*/
WITH cor_tti AS (
    SELECT 		corridor_id,
                week,
                time_bin, 
                sum(tti*seg.length)/sum(seg.length) AS tti, 
                sum(seg.length) AS seg_length, 
                cor.length AS cor_length
    
    FROM 		congestion.segments_tti_weekly 
    INNER JOIN 	congestion.segments_v5 seg using (segment_id)
    INNER JOIN 	congestion.corridors_v1_merged_lookup using (segment_id)
    INNER JOIN 	congestion.corridors_v1_merged cor using (corridor_id)
    
    GROUP BY 	corridor_id, week, time_bin, cor.length
)

/*
Final Output: Creates an estimate of the corridor TTI into congestion.corridor_tti_week, where at least 80% of the corridor (by distance) has observations at the segment (segment_id) level
*/
SELECT 			corridor_id,
                week,
                time_bin, 
                CASE WHEN cor_length*0.8 < seg_length 
                        THEN tti 
                    ELSE 
                        NULL 
                END as tti
                
FROM 			cor_tti 

WHERE 			time_bin <@ '[06:00:00, 23:00:00)'::timerange

GROUP BY 		corridor_id,  week,  time_bin, cor_length, seg_length, tti
ORDER BY 		corridor_id,  week,  time_bin;

ALTER TABLE congestion.corridor_tti_weekly
    OWNER TO congestion_admins;
