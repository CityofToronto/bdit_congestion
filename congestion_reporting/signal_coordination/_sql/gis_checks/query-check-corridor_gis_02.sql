-- check to ensure that links are not duplicated (i.e not showing up in more than one corridor

SELECT 		C.group_id,
		A.link_dir,
		COUNT(*) AS num_links

FROM 		here_analysis.corridor_links A
INNER JOIN	here_analysis.corridors C USING (corridor_id)
WHERE 		C.group_id IN (aa, bb, cc, dd) -- corresponding group IDs
GROUP BY	C.group_id, A.link_dir
HAVING		COUNT(*) > 1