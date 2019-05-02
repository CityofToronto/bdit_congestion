CREATE MATERIALIZED VIEW here_analysis.ttr_period_neighbourhood AS
SELECT 		C.area_name,
		ST_Transform(C.geom,98012) AS geom,
		A.mth,
		A.period_id,
		SUM(A.ttr*ST_Length(B.geom))/SUM(ST_Length(B.geom)) AS ttr

FROM		here_analysis.ttr_period_links A
INNER JOIN	here_analysis.here_gis B USING (link_dir)
INNER JOIN	gis.to_neighbourhood C ON ST_Intersects(ST_Transform(C.geom,98012),B.geom)

GROUP BY	C.area_name,
		ST_Transform(C.geom,98012),
		A.mth,
		A.period_id