CREATE TABLE congestion.network_baseline AS

-- Aggregate travel time for each link up to 30 mins 
WITH link_60_tt AS (
	SELECT 		segment_id,
				link_dir,
				datetime_bin(tx, 60)A datetime_bin,
				avg(links.length * 0.001/ mean * 3600) AS link_tt,
				links.length
				
	FROM 		here.ta
	INNER JOIN 	congestion.network_links_21_1 links USING (link_dir)
	LEFT JOIN 	ref.holiday hol ON hol.dt = tx::date
	WHERE 		tx >= '2019-01-01 00:00:00' AND tx < '2020-01-01 00:00:00' AND -- Only aggregating free flow tt using 2019 data 
				hol.dt IS NULL AND -- exclude holiday dates
				date_part('isodow'::text, tx)::integer < 6 AND -- include only weekdays 
				confidence >=30 -- only use high confidence data
	
	GROUP BY	segment_id, link_dir, datetime_bin, links.length)
	
, segment_60_tt AS (
	SELECT		segment_id,
				datetime_bin,
				total_length / (sum(link_60_tt.length) / sum(link_60_tt.link_tt)) AS segment_tt_avg
				
	FROM		link_60_tt
	INNER JOIN 	congestion.network_segments USING (segment_id)
	
	GROUP BY 	datetime_bin, segment_id, total_length
	HAVING 		sum(link_60_tt.length) >= (total_length * 0.8) )-- where at least 80% of links have data
	
SELECT 		segment_id, 
			PERCENTILE_CONT (0.10) WITHIN GROUP (ORDER BY segment_tt_avg ASC) AS baseline_10pct,
			PERCENTILE_CONT (0.15) WITHIN GROUP (ORDER BY segment_tt_avg ASC) AS baseline_15pct,
			PERCENTILE_CONT (0.20) WITHIN GROUP (ORDER BY segment_tt_avg ASC) AS baseline_20pct,
			PERCENTILE_CONT (0.25) WITHIN GROUP (ORDER BY segment_tt_avg ASC) AS baseline_25pct

FROM 		segment_60_tt
WHERE 		datetime_bin::time >= '07:00:00' AND datetime_bin::time  < '21:00:00'
GROUP BY 	segment_id;

