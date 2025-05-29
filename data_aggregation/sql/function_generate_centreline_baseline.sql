CREATE OR REPLACE FUNCTION congestion.generate_centreline_baseline(
    _yr date
)
RETURNS void
LANGUAGE sql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

WITH link_60_tt AS (
	SELECT 		segment_id,
				link_dir,
				ta.dt,
				extract(hour from tod)::int AS hr,
				avg(links.length * 0.001/ mean * 3600) AS link_tt,
				links.length
				
	FROM 		here.ta ta
	INNER JOIN 	congestion.network_links_22_2 links USING (link_dir)
	LEFT JOIN 	ref.holiday hol ON hol.dt = ta.dt
	WHERE 		ta.dt >= _yr AND ta.dt < _yr + interval '1 year' AND -- Only aggregating free flow tt using 2019 data 
				hol.dt IS NULL AND -- exclude holiday dates
				date_part('isodow'::text, ta.dt)::integer < 6 AND -- include only weekdays 
				confidence >= 30 -- only use high confidence data 
	
	GROUP BY	segment_id, link_dir, ta.dt, hr, links.length)
	
, segment_60_tt AS (
	SELECT		segment_id,
				dt,
				hr,
				total_length / (sum(link_60_tt.length) / sum(link_60_tt.link_tt)) AS segment_tt_avg,
				total_length AS seg_length
				
	FROM		link_60_tt
	INNER JOIN 	congestion.network_segments USING (segment_id)
	
	GROUP BY 	dt, hr, segment_id, total_length
	HAVING 		sum(link_60_tt.length) >= (total_length * 0.8))-- where at least 80% of links have data

, centreline_set_60_tt AS (
	SELECT 		uid, 
				dt,
				hr,
				((segment_set_length / (sum(seg_length) / sum(segment_tt_avg))) * cent_length)/segment_set_length as tt

	FROM		segment_60_tt
	INNER JOIN	congestion.segment_centreline_lookup USING (segment_id)
	GROUP BY	uid, dt, hr, segment_set_length, cent_length
	HAVING 		sum(seg_length) >= (segment_set_length * 0.8))

INSERT INTO congestion.centreline_baseline	
SELECT 		uid, 
            _yr AS yr,
			PERCENTILE_CONT (0.10) WITHIN GROUP (ORDER BY tt ASC) AS baseline_10pct,
			PERCENTILE_CONT (0.15) WITHIN GROUP (ORDER BY tt ASC) AS baseline_15pct,
			PERCENTILE_CONT (0.20) WITHIN GROUP (ORDER BY tt ASC) AS baseline_20pct,
			PERCENTILE_CONT (0.25) WITHIN GROUP (ORDER BY tt ASC) AS baseline_25pct

FROM 		centreline_set_60_tt
WHERE 		hr>= 7 AND hr < 21
GROUP BY 	uid;

$BODY$;

ALTER FUNCTION congestion.generate_centreline_monthly(date)
OWNER TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.generate_centreline_monthly(date) TO congestion_admins;
GRANT EXECUTE ON FUNCTION congestion.generate_centreline_monthly(date) TO congestion_bot;
COMMENT ON FUNCTION congestion.generate_centreline_monthly(date)
IS 'Function that aggregate centreline equivalent of network segments baseline travel time for each year, excluding holidays.';


