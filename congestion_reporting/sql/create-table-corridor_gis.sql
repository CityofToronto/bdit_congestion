DROP TABLE IF EXISTS here_analysis.corridor_gis;

CREATE TABLE 	here_analysis.corridor_gis (
		corridor_id integer,
		direction text,
		geom geometry(MultiLineString,32190)
		);

INSERT INTO 	here_analysis.corridor_gis(corridor_id, direction, geom)
SELECT 		X.corridor_id,
		X.direction,
		ST_Transform(ST_Multi(ST_Union(geom)),32190) AS geom
FROM		(	SELECT 		A.corridor_id,
					C.direction,
					A.seq, 
					(CASE WHEN RIGHT(A.link_dir,1) = 'F' THEN B.geom ELSE ST_Reverse(B.geom) END) as geom
			FROM 		here_analysis.corridor_links A
			INNER JOIN 	here_gis.streets_16_1 B ON LEFT(A.link_dir,-1)::numeric = B.link_id
			INNER JOIN	here_analysis.corridors C USING (corridor_id)
			WHERE 		((corridor_id >= 161 AND corridor_id <= 281) OR (corridor_id >= 436 AND corridor_id <= 550))
			ORDER BY 	A.corridor_id, C.direction, A.seq
		) X
GROUP BY 	X.corridor_id, X.direction
ORDER BY 	X.corridor_id;