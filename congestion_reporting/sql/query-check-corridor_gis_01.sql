
SELECT 		X.group_id,
		X.street,
		X.corridor_id,
		X.direction,
		ST_AsText(ST_LineMerge(ST_Multi(ST_Union(geom)))) AS X
FROM		(	SELECT 		C.group_id,
					A.corridor_id,
					C.street,
					C.direction,
					A.seq, 
					(CASE WHEN RIGHT(A.link_dir,1) = 'F' THEN B.geom ELSE ST_Reverse(B.geom) END) as geom
			FROM 		here_analysis.corridor_links A
			INNER JOIN 	here_gis.streets_18_3 B ON LEFT(A.link_dir,-1)::numeric = B.link_id
			INNER JOIN	here_analysis.corridors C USING (corridor_id)
			WHERE 		C.group_id IN (47,48,49,50,51,52,53,54)
			ORDER BY 	A.corridor_id, C.direction, A.seq
		) X
GROUP BY 	X.group_id, X.street, X.corridor_id, X.direction
ORDER BY 	X.group_id, X.corridor_id;