-- TRUNCATE here_analysis.corridor_link_agg;

-- INSERT INTO here_analysis.corridor_link_agg (corridor_id, link_dir, yyyymm, hh, tt)

SELECT C.corridor_id, B.link_dir, date_trunc('month',A.tx) as yyyymm, EXTRACT(HOUR from A.tx) as hh, ST_Length(ST_Transform(D.geom,32190)) * AVG(1.0/A.pct_50) * 3.6 AS tt
FROM here.ta A
INNER JOIN here_analysis.corridor_links B USING (link_dir)
INNER JOIN here_analysis.corridors C USING (corridor_id)
INNER JOIN here_gis.streets_16_1 D ON LEFT(B.link_dir,-1)::numeric = D.link_id
WHERE C.corridor_id IN (55,56,57,58) AND A.tx >= '2017-06-01' AND A.tx < '2017-07-01' AND EXTRACT(dow FROM A.tx) IN (1,2,3,4,5) AND A.tx::date NOT IN ('2017-06-30','2017-04-14','2017-04-17','2017-05-22')
GROUP BY C.corridor_id, B.link_dir, date_trunc('month',A.tx), EXTRACT(hour from A.tx), ST_Length(ST_Transform(D.geom,32190));