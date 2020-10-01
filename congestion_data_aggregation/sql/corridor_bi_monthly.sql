/* 
MATERIALIZED VIEW: congestion.corridor_bi_monthly
PURPOSE: This materialized view contains estimates of a corridor-level buffer index (BI) for every hour of the day, every week
*/

CREATE MATERIALIZED VIEW congestion.corridor_bi_monthly AS

/*
cor_bi: Produces estimates of the buffer index (BI) for each month for each hour by corridor (corridor_id)
*/
WITH cor_bi AS (
	SELECT 		corridor_id, 
                month, 
                time_bin, 
                sum(bi*seg.length)/cor.length AS bi, 
                sum(seg.length) AS seg_length, 
                cor.length AS cor_length
    
	FROM 		congestion.segments_bi_monthly
	JOIN 		congestion.segments_v5 seg using (segment_id)
	JOIN 		congestion.corridors_v1_merged_lookup using (segment_id)
	JOIN 		congestion.corridors_v1_merged cor using (corridor_id)
    
	GROUP BY corridor_id, month, time_bin, cor.length
)

/*
Final Output: Inserts an estimate of the corridor buffer index (BI) into congestion.corridor_bi_hourly, where at least 80% of the corridor (by distance) has observations at the segment (segment_id) level
*/
SELECT 			corridor_id, 
                month, 
                time_bin, 
                CASE WHEN cor_length*0.8 < seg_length 
                         THEN bi 
                    ELSE 
                         NULL 
                END AS bi
                
FROM 			cor_bi 

WHERE 			time_bin <@ '[06:00:00, 23:00:00)'::timerange

GROUP BY 		corridor_id, month, time_bin, cor_length, seg_length, bi
ORDER BY 		corridor_id, month, time_bin



