SELECT		group_id,
		corridor_id,
		concat(street,' ',direction) AS main_street,
		intersection_start AS from_cross_street, 
		intersection_end AS to_cross_street, 
		length_km AS distance_km,
		C.month_bin,
		hh,
		round(SUM(tt_avg),1) AS tt,
		num_links,
		AVG(obs) AS avg_obs

FROM		here_analysis.corridors A
INNER JOIN	here_analysis.corridor_links B USING (corridor_id)
INNER JOIN	here_analysis.corridor_link_month C USING (corridor_id, link_dir)
WHERE		hh >= 6 AND hh < 22

GROUP BY 	group_id, group_order, corridor_id, concat(street,' ',direction), intersection_start, intersection_end, length_km, C.month_bin, hh, num_links
HAVING		COUNT(*) = num_links
ORDER BY 	group_id, group_order, hh, month_bin