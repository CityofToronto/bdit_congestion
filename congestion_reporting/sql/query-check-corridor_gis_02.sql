SELECT 		C.group_id,
		A.link_dir,
		COUNT(*) AS num_links

FROM 		here_analysis.corridor_links A
INNER JOIN	here_analysis.corridors C USING (corridor_id)
WHERE		C.group_id > 94
GROUP BY	C.group_id, A.link_dir
HAVING		COUNT(*) > 1