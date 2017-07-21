UPDATE 		here_analysis.corridor_links_15min A
SET		spd_avg = here_analysis.estimate_speed	(	corridor_id, 
								here_analysis.closest_valid_link(	corridor_id,
													seq::smallint,
													datetime_bin
													),
								seq::smallint,
								here_analysis.closest_valid_link_bin(	corridor_id,
													seq::smallint,
													datetime_bin
													)
							)
WHERE 		estimated = TRUE;