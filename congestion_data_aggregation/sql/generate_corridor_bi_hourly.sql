-- FUNCTION: congestion.generate_corridor_bi_hourly(date)

-- DROP FUNCTION congestion.generate_corridor_bi_hourly(date);

CREATE OR REPLACE FUNCTION congestion.generate_corridor_bi_hourly(
	_dt date)
    RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$
with speed_links as (
                    select segment_id, link_dir,
							datetime_bin(tx,60) AS datetime_bin,
							harmean(mean) AS spd_avg_all, 
							length AS link_length,
							COUNT (DISTINCT tx)  AS count_hc
					from here.ta
                    inner join congestion.segment_links_v5_19_4 using (link_dir)
					LEFT JOIN ref.holiday hol ON hol.dt = tx::date
                    where hol.dt IS NULL AND date_part('isodow'::text, tx::date) < 6
				and (tx >= _dt AND tx < ( _dt + '1 mon'::interval))
					group by segment_id,link_dir,datetime_bin(tx,60), link_length
					)	
, seg_tt as (
				SELECT segment_id, datetime_bin,
				CASE WHEN SUM(link_length) >= 0.8 * b.length 
					THEN SUM(link_length / spd_avg_all  * 3.6 ) * b.length / SUM(link_length)
					END AS spd_avg_all,	
					SUM(link_length) / b.length * 100 AS data_pct_hc
				FROM speed_links
				INNER JOIN congestion.segments_v5 b 
				USING (segment_id)
				WHERE link_length / spd_avg_all  IS NOT NULL
				GROUP BY segment_id,  datetime_bin, b.length
				ORDER BY segment_id,  datetime_bin)
, seg_bi as (				
SELECT a.segment_id,
            date_trunc('month', datetime_bin) as month,
            datetime_bin::time without time zone AS time_bin,
            count(a.datetime_bin) AS num_bins,
            avg(a.spd_avg_all) AS avg_tt,
			percentile_cont(0.95::double precision) WITHIN GROUP (ORDER BY a.spd_avg_all) as pct_95,
            (percentile_cont(0.95::double precision) WITHIN GROUP (ORDER BY a.spd_avg_all) - avg(a.spd_avg_all))/ avg(a.spd_avg_all) AS bi
           FROM seg_tt a
           WHERE a.spd_avg_all IS NOT NULL
          GROUP BY a.segment_id, (a.datetime_bin::time without time zone), date_trunc('month', datetime_bin)
          ORDER BY a.segment_id, date_trunc('month', datetime_bin), (a.datetime_bin::time without time zone))

,cor_bi as (
select corridor_id, month, time_bin, sum(bi*seg.length)/cor.length as bi, sum(seg.length) as seg_length, cor.length as cor_length
from seg_bi
join congestion.segments_v5 seg using (segment_id)
join congestion.corridors_v1_merged_lookup using (segment_id)	
join congestion.corridors_v1_merged cor using (corridor_id)	
group by corridor_id, month, time_bin, cor.length	
)
insert into congestion.corridor_bi_hourly
select corridor_id, month, time_bin, case when cor_length*0.8 < seg_length then bi else null end as bi
FROM cor_bi 
WHERE time_bin <@ '[07:00:00, 23:00:00)'::timerange
GROUP BY corridor_id, month, time_bin, cor_length, seg_length, bi
ORDER BY corridor_id, month, time_bin

$BODY$;

ALTER FUNCTION congestion.generate_corridor_bi_hourly(date)
    OWNER TO natalie;
