﻿-- Check to ensure that grouped corridors are continuous (i.e. should be LineString not MultiLineString)

SELECT 		X.group_id,
		X.street,
		X.direction,
		ST_AsText(ST_LineMerge(ST_Multi(ST_Union(geom)))) AS X
FROM		(	SELECT 		C.group_id,
					C.street,
					C.direction,
					(CASE WHEN RIGHT(A.link_dir,1) = 'F' THEN B.geom ELSE ST_Reverse(B.geom) END) as geom
			FROM 		here_analysis.corridor_links A
			INNER JOIN 	here_gis.streets_18_3 B ON LEFT(A.link_dir,-1)::numeric = B.link_id
			INNER JOIN	here_analysis.corridors C USING (corridor_id)
			WHERE 		C.group_id IN (aa, bb, cc, dd) -- corresponding group IDs
			ORDER BY 	C.direction, A.seq
		) X
GROUP BY 	X.group_id, X.street, X.direction
ORDER BY 	X.group_id;