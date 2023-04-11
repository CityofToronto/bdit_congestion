-- FUNCTION: congestion.generate_citywide_tti_daily(date)

-- DROP FUNCTION congestion.generate_citywide_tti_daily(date);

CREATE OR REPLACE FUNCTION congestion.generate_citywide_tti_daily(
	_dt date)
    RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$

WITH speed_links AS (
	SELECT 		segment_id, 
				link_dir,
				length AS link_length, 
				(TIMESTAMP WITHOUT TIME ZONE 'epoch' +
                    INTERVAL '1 second' * (floor((extract('epoch' from tx)) / 1800) * 1800)) AS datetime_bin,
				harmean(mean) AS spd_avg,
				COUNT(DISTINCT tx)  AS count
	
	FROM  		here.ta
    INNER JOIN 	congestion.segment_links_v6_21_1 USING (link_dir)
	
	WHERE 	    (tx >=  _dt AND tx < _dt + INTERVAL '1 day' )

	GROUP BY 	segment_id, link_dir, datetime_bin, length
), 
	

daily AS (
	SELECT 		segment_id, 
				datetime_bin, 
				CASE 	WHEN SUM(link_length) >= 0.8 * b.length 
							THEN SUM(link_length / spd_avg  * 3.6 ) * b.length / SUM(link_length)
						ELSE
							NULL 
				END AS segment_tt_avg 

	FROM 		speed_links
	INNER JOIN 	congestion.segments_v6 b USING (segment_id)

	WHERE 		link_length / spd_avg IS NOT NULL

	GROUP BY 	segment_id, datetime_bin, b.length
	ORDER BY 	segment_id, datetime_bin
),

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
	LEFT JOIN 	congestion.tt_segments_baseline_v6_2019f b USING (segment_id)
	LEFT JOIN 	congestion.highway_segments_v6 highway USING (segment_id)
)

INSERT INTO 	congestion.citywide_tti_daily(dt, time_bin, num_segments, tti)
SELECT 			dt, 
				time_bin, 
				count(seg_tti.segment_id) AS num_segments,
				sum(seg_tti.tti * segments_v5.length * sqrt(segment_aadt_final.aadt))  / sum(segments_v5.length * sqrt(segment_aadt_final.aadt)) AS tti

FROM			seg_tti 
INNER JOIN 		congestion.segments_v6 USING (segment_id)
INNER JOIN		covid.segment_aadt_final USING (segment_id)

WHERE 		    time_bin <@ '[06:00:00, 23:00:00]'::timerange

GROUP BY 		dt, time_bin
ORDER BY 		dt, time_bin

$BODY$;

ALTER FUNCTION congestion.generate_citywide_tti_daily(date)
    OWNER TO congestion_admins;
