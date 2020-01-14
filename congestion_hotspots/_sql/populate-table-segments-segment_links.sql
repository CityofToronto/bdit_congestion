WITH links AS (
	SELECT DISTINCT link_dir
	FROM here.ta_201909 A
)

INSERT INTO congestion.segment_links (segment_id, link_dir)
SELECT ROW_NUMBER() OVER (ORDER BY link_dir) AS segment_id, link_dir
FROM links A
INNER JOIN here_gis.streets_18_3 B ON LEFT(A.link_dir,-1)::numeric = B.link_id
INNER JOIN here_gis.streets_att_18_3 USING (link_id)
WHERE func_class::int IN (1,2,3,4);

INSERT INTO congestion.segments (segment_id, length_m, geom)
SELECT 	segment_id, 
	ST_Length(ST_Transform(ST_Union(CASE WHEN RIGHT(link_dir,1) = 'T' THEN ST_Reverse(B.geom) ELSE B.geom END),2952)) AS length_m, 
	ST_Transform(ST_Union(CASE WHEN RIGHT(link_dir,1) = 'T' THEN ST_Reverse(B.geom) ELSE B.geom END),2952) AS geom
FROM congestion.segment_links A
INNER JOIN here_gis.streets_18_3 B ON LEFT(A.link_dir,-1)::numeric = B.link_id
GROUP BY segment_id;