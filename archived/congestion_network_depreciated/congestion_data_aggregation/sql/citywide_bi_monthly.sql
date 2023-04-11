/* 
MATERIALIZED VIEW: congestion.citywide_bi_monthly
PURPOSE: This materialized view contains estimates of a city-wide buffer index (BI) for every hour of the day, every week
*/

CREATE MATERIALIZED VIEW congestion.citywide_bi_monthly AS

/*
Final Output: Creates an estimate of the city-wide BI (weighted by length of segment and sqrt of AADT)
*/
SELECT			month, 
            	time_bin, 
            	count(segment_id) AS num_segments,
            	sum(bi * segments_v5.length * segment_aadt_final.aadt)/sum(segments_v5.length * segment_aadt_final.aadt)  AS bi

FROM 	   		congestion.segments_bi_monthly
INNER join 		congestion.segments_v5 using (segment_id)
INNER join 		covid.segment_aadt_final USING (segment_id) 

WHERE 	    	time_bin <@ '[06:00:00,23:00:00)'::timerange

GROUP BY		month, time_bin
ORDER BY 		month, time_bin


ALTER TABLE congestion.citywide_bi_monthly
    OWNER TO congestion_admins;

CREATE UNIQUE INDEX citywide_bi_monthly_month_time_bin_unique
    ON congestion.citywide_bi_monthly USING btree
    (month, time_bin)
