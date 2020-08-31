-- FUNCTION: congestion.generate_corridor_tti_hourly(date)

-- DROP FUNCTION congestion.generate_corridor_tti_hourly(date);

CREATE OR REPLACE FUNCTION congestion.generate_corridor_tti_hourly(
	_dt date)
    RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$

with speed_links as (
                    select segment_id, 
					link_dir,
					length AS link_length, 
					date_trunc('hour', tx) AS datetime_bin,
					harmean(mean) AS spd_avg,
					COUNT (DISTINCT tx)  AS count_hc
					from  here.ta
                    inner join congestion.segment_links_v5_19_4 using (link_dir)
					LEFT JOIN ref.holiday hol ON hol.dt = tx::date
                    where hol.dt IS NULL AND date_part('isodow'::text, tx::date) < 6 and (tx >=  _dt AND tx < (  $1 + '1 mon'::interval))
			    	
				 	GROUP BY segment_id, link_dir, datetime_bin, length)
, hourly_tti as (SELECT segment_id, datetime_bin, 
CASE WHEN SUM(link_length) >= 0.8 * b.length 
	 THEN SUM(link_length / spd_avg  * 3.6 ) * b.length / SUM(link_length)
	 ELSE NULL 
	 END AS segment_tt_avg 
FROM speed_links
INNER JOIN congestion.segments_v5 b 
USING (segment_id)
WHERE link_length / spd_avg  IS NOT NULL
GROUP BY segment_id, datetime_bin, b.length
ORDER BY segment_id, datetime_bin)

, monthly as (select segment_id, date_trunc('month', datetime_bin) as month, datetime_bin::time without time zone as time_bin,
			avg(segment_tt_avg) as segment_tt_avg from hourly_tti
			 where segment_tt_avg is not null 
			group by month, time_bin,segment_id )

, seg_tti as (SELECT segment_id, month, time_bin,  
				case when highway.segment_id is not null then tti.segment_tt_avg/b.tt_baseline_10pct_corr 
							else tti.segment_tt_avg/b.tt_baseline_25pct_corr end AS tti
				from monthly tti
				LEFT JOIN congestion.tt_segments_baseline_v5_2019_af b USING (segment_id)
				left join congestion.highway_segments_v5 highway using (segment_id))
				

, cor_tti as (
select corridor_id, month, time_bin, sum(tti*seg.length)/cor.length as tti, sum(seg.length) as seg_length, cor.length as cor_length
from seg_tti
join congestion.segments_v5 seg using (segment_id)
join congestion.corridors_v1_merged_lookup using (segment_id)	
join congestion.corridors_v1_merged cor using (corridor_id)	
group by corridor_id, month, time_bin, cor.length	
)
insert into congestion.corridor_tti_hourly
select corridor_id, month, time_bin, case when cor_length*0.8 < seg_length then tti else null end as tti
FROM cor_tti 
WHERE time_bin <@ '[07:00:00, 23:00:00)'::timerange
GROUP BY corridor_id, month, time_bin, cor_length, seg_length, tti
ORDER BY corridor_id, month, time_bin

$BODY$;

ALTER FUNCTION congestion.generate_corridor_tti_hourly(date)
    OWNER TO natalie;
