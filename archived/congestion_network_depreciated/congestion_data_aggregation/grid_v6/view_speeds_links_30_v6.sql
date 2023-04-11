create view congestion.speeds_links_30_v6 as 

                    with here_prep as (
                        select link_dir, tx, stddev, confidence, mean, pct_50
                        from  here.ta
                        inner join congestion.segment_links_v6_21_1 using (link_dir)
						where tx >= '2019-01-01' and tx <'2020-01-01')
                        
                    , here as (
                        select * from here_prep
                        LEFT JOIN ref.holiday hol ON hol.dt = here_prep.tx::date
                        where hol.dt IS NULL AND date_part('isodow'::text, tx::date)::integer < 6)	

                    SELECT  X.segment_id, 
                            X.link_dir, 
                            X.datetime_bin, 
                            X.spd_avg_all, 
                            Y.spd_avg_hc, 
                            X.spd_med_all,  
                            Y.spd_med_hc,
                            X.count_all, 
                            Y.count_hc
                    FROM
                    (
                        SELECT a.segment_id, a.link_dir, 
                        (datetime_bin(b.tx,30)) AS datetime_bin,
                        harmean(mean) AS spd_avg_all,
                        harmean(pct_50) AS spd_med_all, 
                        COUNT (DISTINCT b.tx) AS count_all
                        FROM congestion.segment_links_v6_21_1 a
                        INNER JOIN here b
                        USING (link_dir)
                        GROUP BY a.segment_id, a.link_dir, datetime_bin
                    ) X

                    LEFT JOIN

                    (
                        SELECT a.segment_id, a.link_dir, 
                        (datetime_bin(b.tx,30)) AS datetime_bin,
                        harmean(mean) AS spd_avg_hc,
                        harmean(pct_50) AS spd_med_hc,
                        COUNT (DISTINCT b.tx)  AS count_hc
                        FROM congestion.segment_links_v6_21_1 a
                        INNER JOIN here b
                        USING (link_dir)
                        WHERE confidence >= 30
                        GROUP BY a.segment_id, a.link_dir, datetime_bin
                        )  Y

                    USING (segment_id, link_dir, datetime_bin)
                    ORDER BY segment_id, link_dir, datetime_bin