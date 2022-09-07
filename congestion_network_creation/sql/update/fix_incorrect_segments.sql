-----------------------------------------------------------------------------------------------------------------------------------------
-- delete incorrect node
DELETE FROM congestion.network_nodes
WHERE node_id = 871132743;

-- insert correct node
INSERT INTO congestion.network_nodes 
SELECT DISTINCT node_id, geom 
FROM here.routing_nodes_21_1
WHERE node_id = 30362017;
-----------------------------------------------------------------------------------------------------------------------------------------
-- delete incorrect px int node lookup
DELETE FROM congestion.network_int_px_21_1
WHERE node_id = 871132743;

-- insert correct px int node lookup
INSERT INTO  congestion.network_int_px_21_1
SELECT distinct 30362017, 13466357 , '2487', node.geom, cent.geom, null::double precision as dist
FROM congestion.network_nodes node, gis.centreline_intersection_20220705 cent
where node.node_id = 30362017 and int_id = 13466357
-----------------------------------------------------------------------------------------------------------------------------------------
-- delete incorrect segments 
DELETE FROM congestion.network_segments
where segment_id in (675, 4506, 1297, 3563);

DELETE FROM congestion.network_links_21_1
where segment_id in (675, 4506, 1297, 3563);

-----------------------------------------------------------------------------------------------------------------------------------------
--REROUTE

-- Insert re-route result to network_routing_results
WITH intersections as(
	SELECT  30363407 as start_vid , 30363400 as end_vid
), results AS(
	SELECT results.*, link_dir, start_vid, end_vid, routing_grid.id, routing_grid.geom
	FROM intersections
	, LATERAL pgr_dijkstra('SELECT id, source::int, target::int,
						   st_length(st_transform(geom, 2952)) as cost
						   FROM here.routing_streets_21_1 routing_grid
						   ',
				start_vid, end_vid) results
	INNER JOIN here.routing_streets_21_1 routing_grid ON id = edge
)
insert into congestion.network_links_21_1
SELECT 675  as segment_id, start_vid, end_vid, link_dir , geom, cost as length
FROM results;


insert into  congestion.network_segments
select segment_id, start_vid, end_vid, ST_linemerge(ST_union(geom)) , sum(length), false
from  congestion.network_links_21_1 
where segment_id = 675
group by segment_id, start_vid, end_vid;
-- repeat for the other 3 segments

-----------------------------------------------------------------------------------------------------------------------------------------

-- delete from baseline
DELETE FROM congestion.network_baseline
where segment_id in (675, 4506, 1297, 3563);

-- recalculate baseline

WITH link_60_tt AS (
	SELECT 		segment_id,
				link_dir,
				datetime_bin(tx, 60) AS datetime_bin,
				avg(links.length * 0.001/ mean * 3600) AS link_tt,
				links.length
				
	FROM 		here.ta
	INNER JOIN 	congestion.network_links_21_1 links USING (link_dir)
	LEFT JOIN 	ref.holiday hol ON hol.dt = tx::date
	WHERE 		tx >= '2019-01-01 00:00:00' AND tx < '2020-01-01 00:00:00' AND -- Only aggregating free flow tt using 2019 data 
				hol.dt IS NULL AND -- exclude holiday dates
				date_part('isodow'::text, tx)::integer < 6 AND -- include only weekdays 
				confidence >=30 -- only use high confidence data 
	and segment_id in (675, 4506, 1297, 3563)
	
	GROUP BY	segment_id, link_dir, datetime_bin, links.length)
	
, segment_60_tt AS (
	SELECT		segment_id,
				datetime_bin,
				total_length / (sum(link_60_tt.length) / sum(link_60_tt.link_tt)) AS segment_tt_avg
				
	FROM		link_60_tt
	INNER JOIN 	congestion.network_segments USING (segment_id)
	
	GROUP BY 	datetime_bin, segment_id, total_length
	HAVING 		sum(link_60_tt.length) >= (total_length * 0.8) )-- where at least 80% of links have data

INSERT INTO congestion.network_baseline	
SELECT 		segment_id, 
			PERCENTILE_CONT (0.10) WITHIN GROUP (ORDER BY segment_tt_avg ASC) AS baseline_10pct,
			PERCENTILE_CONT (0.15) WITHIN GROUP (ORDER BY segment_tt_avg ASC) AS baseline_15pct,
			PERCENTILE_CONT (0.20) WITHIN GROUP (ORDER BY segment_tt_avg ASC) AS baseline_20pct,
			PERCENTILE_CONT (0.25) WITHIN GROUP (ORDER BY segment_tt_avg ASC) AS baseline_25pct

FROM 		segment_60_tt
WHERE 		datetime_bin::time >= '07:00:00' AND datetime_bin::time  < '21:00:00'
GROUP BY 	segment_id;

-----------------------------------------------------------------------------------------------------------------------------------------
-- Delete daily and monthly data for affected segment_ids
DELETE FROM  congestion.network_segments_daily  
where segment_id in (675, 4506, 1297, 3563);

DELETE FROM  congestion.network_segments_monthly 
where segment_id in (675, 4506, 1297, 3563);
-----------------------------------------------------------------------------------------------------------------------------------------

WITH speed_links AS (
    SELECT 		segment_id, 
				link_dir,
				links.length AS link_length,
				dt, 
				extract(hour from tod)::int AS hr,
				harmean(mean) AS spd_avg,
				COUNT(tx)::int as num_bin
    
    FROM  		here.ta
    INNER JOIN 	congestion.network_links_22_2 links USING (link_dir)
    WHERE 		(dt >= '2017-09-01' AND dt < '2022-08-28') and segment_id in (675, 4506, 1297, 3563)
    
	GROUP BY    segment_id, link_dir, dt, hr, links.length
),

/*
tt_hr: Produces estimates of the average travel time for each 1 hour bin for each individual segment (segment_id), 
		where at least 80% of the segment (by distance) has observations at the link (link_dir) level
*/
tt_hr AS (
    SELECT 		segment_id, 
                dt,
				hr,
                CASE WHEN SUM(link_length) >= 0.8 * total_length 
                          THEN SUM(link_length / spd_avg  * 3.6 ) * total_length / SUM(link_length)
                     ELSE 
                         NULL 
                END AS segment_avg_tt,
				CASE WHEN highway IS false 
						THEN baseline_10pct
					ELSE
						baseline_25pct
				END AS baseline_tt,
				sum(num_bin) as num_bin
    
    FROM 		speed_links
    INNER JOIN 	congestion.network_segments USING (segment_id)
    LEFT JOIN 	congestion.network_baseline USING (segment_id)
	
    GROUP BY	segment_id, dt, hr, total_length, highway, baseline_10pct, baseline_25pct
    ORDER BY	segment_id, dt, hr
)

/*
Final Output: Inserts an estimate of the segment aggregation into congestion.network_segments_daily
*/
INSERT INTO     congestion.network_segments_daily      
SELECT 			segment_id,
                dt,
                hr,
                round(segment_avg_tt::numeric, 2) as tt,
				num_bin
FROM 			tt_hr
WHERE 			segment_avg_tt IS NOT NULL;


INSERT INTO congestion.network_segments_monthly
	SELECT 		segment_id, 
				date_trunc('month', dt) AS mth, 
				hr,
				CASE WHEN extract(isodow from dt) <6 then 'Weekday'
					ELSE 'Weekend' END AS day_type,
				round(avg(tt), 2) AS avg_tt,
				PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tt) AS median_tt,
				PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY tt) AS pct_85_tt,
				PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tt) AS pct_95_tt,
				round(min(tt), 2) AS min_tt,
				round(max(tt), 2) AS max_tt,
				stddev(tt) as std_dev,
				sum(num_bins)::int AS num_bins
    
    FROM  		congestion.network_segments_daily
	LEFT JOIN 	ref.holiday USING (dt) -- exclude holidays
    WHERE 		(dt >= '2017-09-01'  AND dt < '2022-08-01' ) and 
				holiday.dt IS NULL and segment_id in (675, 4506, 1297, 3563)
    
	GROUP BY    segment_id, mth, hr, day_type
	ORDER BY    segment_id, mth, hr, day_type;


-----------------------------------------------------------------------------------------------------------------------------------------
-- save as .sql and run with nohup 
nohup psql -U natalie -d bigdata -h ip -f here_congestion_incorrect.sql &