-----------------------------------------------------------------------------------------------------------------------------------------
-- SQL to Update segments that got split by new traffic signals 
-----------------------------------------------------------------------------------------------------------------------------------------
-- Find segments that needs to be updated due to new traffic signals
with new_signal as (
	select px, ST_Transform(ST_buffer(ST_Transform(geom, 2952), 50), 4326) as geom 
	from gis.traffic_signal
	left join congestion.network_int_px_22_2 a using (px)
	where activationdate >= '2022-04-17' and a.px is null )

select distinct px
from congestion.network_segments seg
join new_signal on ST_intersects(new_signal.geom, seg.geom);

-----------------------------------------------------------------------------------------------------------------------------------------
-- Add new nodes where the traffic signal lies using nearest neighbour
-- double check on QGIS to see if the matches are good first
with new_signal as (
	select px, ST_Transform(ST_buffer(ST_Transform(geom, 2952), 50), 4326) as bgeom, geom 
	from gis.traffic_signal
	left join congestion.network_int_px_22_2 a using (px)
	where activationdate >= '2022-04-17' and a.px is null )

, new_px AS (
	select distinct px, new_signal.geom as px_geom
	from congestion.network_segments seg
	join new_signal on ST_intersects(new_signal.bgeom, seg.geom))

-- Find new nodes for outdated nodes using nearest neighbour
insert into  congestion.network_nodes_23			
select 	distinct node_id, (ST_Dump(geom)).geom
from 	 new_px
CROSS JOIN LATERAL (SELECT node_id,
							geom, 
							(ST_transform(geom, 2952) <-> ST_Transform(px_geom, 2952)) as dist	
					FROM here.routing_nodes_23_4
					ORDER BY (geom <-> px_geom)
					LIMIT 1) nodes;

-----------------------------------------------------------------------------------------------------------------------------------------
-- create new network_int_px lookup table for the current map version
CREATE TABLE congestion.network_int_px_22_2 AS 

SELECT * FROM congestion.network_int_px_21_1 -- because no nodes changed in this version

-- add new traffic signal nodes to table
insert into congestion.network_int_px_23_4
with new_signal as (
	select px, traffic_signal.node_id as int_id, ST_Transform(ST_buffer(ST_Transform(geom, 2952), 50), 4326) as bgeom, geom as px_geom
	from gis.traffic_signal
	left join congestion.network_int_px_22_2 a using (px) -- where they are not already in old version
	where activationdate >= '2022-04-17' and a.px is null 	
        and px != '2617' -- too far away
    	and px != '2650') -- a ramp
-- px that are new
, new_px AS (
	select distinct px, px_geom, int_id
	from congestion.network_segments seg
	join new_signal on ST_intersects(new_signal.bgeom, seg.geom))

-- nodes that are closest to new traffic signal
, new_nodes AS (
	select 	node_id::int as node_id, px, int_id
	from 	 new_px
	CROSS JOIN LATERAL (SELECT node_id,
								geom, 
								(ST_transform(geom, 2952) <-> ST_Transform(px_geom, 2952)) as dist	
						FROM here.routing_nodes_23_4
						ORDER BY (geom <-> px_geom)
						LIMIT 1) nodes)
, temp as (
select start_vid as node_id from congestion.network_links_23_4
union 
select end_vid from congestion.network_links_23_4)

, missing_nodes AS (
	select distinct node_id, (ST_dump(a.geom)).geom as node_geom
	from congestion.network_int_px_22_2
	right join temp using (node_id)
	inner join here.routing_nodes_23_4 a using (node_id)
	where network_int_px_22_2.node_id is null)
select node_id, case when int_id = 0 then null else int_id end as int_id, px, node_geom, int.geom as ints_geom, null as dist
from missing_nodes
left join new_nodes using (node_id)
left join gis_core.centreline_intersection_point int on intersection_id = int_id


    
-- insert new network_int_px lookup for the nodes we just added
INSERT INTO  congestion.network_int_px_22_2
SELECT distinct node_id, int_id , '2610', node.geom, cent.geom, null::double precision as dist
FROM congestion.network_nodes node, gis.centreline_intersection_20220705 cent
where node.node_id = 30420738 and int_id = 13463084;

INSERT INTO  congestion.network_int_px_22_2
SELECT distinct node_id, int_id , '2588', node.geom, cent.geom, null::double precision as dist
FROM congestion.network_nodes node, gis.centreline_intersection_20220705 cent
where node.node_id in (30454395,30454396)  and int_id = 13451329;

INSERT INTO  congestion.network_int_px_22_2
SELECT distinct node_id, int_id , '2579', node.geom, cent.geom, null::double precision as dist
FROM congestion.network_nodes node, gis.centreline_intersection_20220705 cent
where node.node_id = 968443982 and int_id = 30136534;

INSERT INTO  congestion.network_int_px_22_2
SELECT distinct node_id, int_id , '2601', node.geom, cent.geom, null::double precision as dist
FROM congestion.network_nodes node, gis.centreline_intersection_20220705 cent
where node.node_id = 30421677 and int_id = 13464125;

INSERT INTO  congestion.network_int_px_22_2
SELECT distinct node_id, int_id , '2605', node.geom, cent.geom, null::double precision as dist
FROM congestion.network_nodes node, gis.centreline_intersection_20220705 cent
where node.node_id = 30421675 and int_id = 13464408;

INSERT INTO  congestion.network_int_px_22_2
SELECT distinct node_id, int_id , '2602', node.geom, cent.geom, null::double precision as dist
FROM congestion.network_nodes node, gis.centreline_intersection_20220705 cent
where node.node_id = 30362945 and int_id = 13466492;

INSERT INTO  congestion.network_int_px_22_2
SELECT distinct node_id, int_id , '2565', node.geom, cent.geom, null::double precision as dist
FROM congestion.network_nodes node, gis.centreline_intersection_20220705 cent
where node.node_id = 30358297 and int_id = 13464434;

INSERT INTO  congestion.network_int_px_22_2
SELECT distinct node_id, int_id , '3109', node.geom, cent.geom, null::double precision as dist
FROM congestion.network_nodes node, gis.centreline_intersection_20220705 cent
where node.node_id in (30356236, 30356237)  and int_id = 13447836;

INSERT INTO  congestion.network_int_px_22_2
SELECT distinct node_id, int_id , '2595', node.geom, cent.geom, null::double precision as dist
FROM congestion.network_nodes node, gis.centreline_intersection_20220705 cent
where node.node_id = 30350790 and int_id = 13453545;

INSERT INTO  congestion.network_int_px_22_2
SELECT distinct node_id, int_id , '2611', node.geom, cent.geom, null::double precision as dist
FROM congestion.network_nodes node, gis.centreline_intersection_20220705 cent
where node.node_id = 30359772 and int_id = 13459487;

-----------------------------------------------------------------------------------------------------------------------------------------
-- Route new segments using new nodes we added
WITH nodes AS (
	SELECT array_agg(node_id::int) as nodes_to_route -- aggregate them into one array for many-to-many routing
	FROM	(select start_vid as node_id --start vid of retired segments
			 from congestion.network_segments seg
			 join (select ST_Transform(ST_buffer(ST_Transform(geom, 2952), 50), 4326) as geom from bqu.traffic_signal
					where activationdate >= '2022-04-14') new_signal on ST_intersects(new_signal.geom, seg.geom)
			union 
			select end_vid --end vid of retired segments
				from congestion.network_segments seg
				join (
					select ST_Transform(ST_buffer(ST_Transform(geom, 2952), 50), 4326) as geom from bqu.traffic_signal
					where activationdate >= '2022-04-14') new_signal on ST_intersects(new_signal.geom, seg.geom)
			union
			select distinct node_id FROM  here.routing_nodes_22_2 -- new nodes that we added
			WHERE node_id in (30420738, 30454395, 30454396, 968443982, 30421677, 30421675, 30362945, 30358297, 30356236, 30356237, 30350790))a)

-- routed using many-to-many
, results AS(
	SELECT results.*, link_dir, routing_grid.geom
	FROM nodes
	, LATERAL pgr_dijkstra('SELECT id, source::int, target::int, st_length(st_transform(geom, 2952)) as cost 
						   FROM here.routing_streets_22_2',
				nodes_to_route, nodes_to_route) results
	INNER JOIN here.routing_streets_22_2 routing_grid ON id = edge)

-- assign new segment_id starting at the max number of current segment_id 
, cleaned_results AS (
	SELECT 7056+row_number() over () as segment_id, 
				start_vid, 
				end_vid, 
				array_agg(link_dir order by path_seq) as link_set, 
				st_linemerge(st_union(s.geom)) as geom, 
				sum(cost) as length

	FROM results s
	LEFT OUTER JOIN congestion.network_nodes ON node = node_id AND node != start_vid
	WHERE  not (start_vid = 30421638 and end_vid = 30421633) AND not (start_vid = 30421677 and end_vid = 30421675) AND
		   not (start_vid = 30421675 and end_vid = 30421677) AND not (start_vid = 30421633 and end_vid = 30421638) AND
		   not (start_vid = 30438448 and end_vid = 30438445) AND not (start_vid = 30438445 and end_vid = 30438448) AND
		   not (start_vid = 30421638 and end_vid = 30421675) -- manually cleaning incorrectly routed segments
	GROUP BY start_vid, end_vid
	HAVING COUNT(node_id) =0 and sum(cost) > 20 -- exclude routed results that went pass any other node_ids and short links
	order by start_vid, end_vid)
 
 -- insert into network_links	
INSERT INTO congestion.network_links_22_2 
select 	segment_id, 
		start_vid, 
		end_vid,
		link_dir, 
		geom, 
		round(ST_length(st_transform(geom, 2952))::numeric,2) as length
from (select segment_id, start_vid, end_vid, unnest(link_set) as link_dir
	  from cleaned_results) a
inner join here.routing_streets_22_2 using (link_dir)
order by segment_id;

 -- insert into network_segments	
INSERT INTO congestion.network_segments
select segment_id, start_vid, end_vid, ST_linemerge(ST_union(geom)) , sum(length), false, gis.direction_from_line(ST_linemerge(ST_union(geom))) as dir
from  congestion.network_links_22_2
where segment_id > 7056
group by segment_id, start_vid, end_vid;

-----------------------------------------------------------------------------------------------------------------------------------------
-- Create baseline for these newly created segments
WITH link_60_tt AS (
	SELECT 		segment_id,
				link_dir,
				datetime_bin(tx, 60) AS datetime_bin,
				avg(links.length * 0.001/ mean * 3600) AS link_tt,
				links.length
				
	FROM 		here.ta
	INNER JOIN 	congestion.network_links_22_2 links USING (link_dir)
	LEFT JOIN 	ref.holiday hol ON hol.dt = tx::date
	WHERE 		tx >= '2019-01-01 00:00:00' AND tx < '2020-01-01 00:00:00' AND -- Only aggregating free flow tt using 2019 data 
				hol.dt IS NULL AND -- exclude holiday dates
				date_part('isodow'::text, tx)::integer < 6 AND -- include only weekdays 
				confidence >=30 -- only use high confidence data 
	and segment_id > 7056
	
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
			PERCENTILE_CONT (0.25) WITHIN GROUP (ORDER BY segment_tt_avg ASC) AS baseline_25pct,
            CASE WHEN highway.segment_id IS NOT NULL 
                THEN baseline_10pct 
                ELSE baseline_25pct 
            END AS baseline_tt

FROM 		segment_60_tt
LEFT JOIN   congestion.network_segments_highway AS highway USING (segment_id)
WHERE 		datetime_bin::time >= '07:00:00' AND datetime_bin::time  < '21:00:00' 
GROUP BY 	segment_id;
-----------------------------------------------------------------------------------------------------------------------------------------
-- Often there are segments without baseline cause it made up with old links
-- we will have to recreate those segments using old link

with missing_seg AS (
	select segment_id, start_vid, end_vid
    from (select * from congestion.network_segments_23_4 where segment_id >7113) a
	left join (select * from congestion.network_baseline where segment_id >7113)temp using (segment_id)
	where temp.segment_id is null) -- ones that were missing from the previous step
	
, results AS(
	SELECT segment_id, start_vid, end_vid, link_dir, length
	FROM missing_seg
	, LATERAL pgr_dijkstra('SELECT id, source::int, target::int, st_length(st_transform(geom, 2952)) as cost 
						   FROM here.routing_streets_22_2',
				start_vid, end_vid) results
	INNER JOIN here.routing_streets_22_2 routing_grid ON id = edge)	-- reroute with old link version

, link_60_tt AS (
	SELECT 		segment_id,
				link_dir,
				datetime_bin(tx, 60) AS datetime_bin,
				avg(links.length * 0.001/ mean * 3600) AS link_tt,
				links.length
				
	FROM 		here.ta
	INNER JOIN 	results links USING (link_dir)
	LEFT JOIN 	ref.holiday hol ON hol.dt = tx::date
	WHERE 		ta.dt >= '2019-01-01' AND ta.dt < '2020-01-01' AND -- Only aggregating free flow tt using 2019 data 
				hol.dt IS NULL AND -- exclude holiday dates
				date_part('isodow'::text, ta.dt)::integer < 6 AND -- include only weekdays
				tod >= '07:00:00' AND tod <'21:00:00' AND -- only from 7am to 9pm
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

INSERT INTO congestion.network_baseline	
SELECT 		segment_id, 
			PERCENTILE_CONT (0.10) WITHIN GROUP (ORDER BY segment_tt_avg ASC) AS baseline_10pct,
			PERCENTILE_CONT (0.15) WITHIN GROUP (ORDER BY segment_tt_avg ASC) AS baseline_15pct,
			PERCENTILE_CONT (0.20) WITHIN GROUP (ORDER BY segment_tt_avg ASC) AS baseline_20pct,
			PERCENTILE_CONT (0.25) WITHIN GROUP (ORDER BY segment_tt_avg ASC) AS baseline_25pct,
            CASE WHEN highway.segment_id IS NOT NULL 
                THEN PERCENTILE_CONT (0.10) WITHIN GROUP (ORDER BY segment_tt_avg ASC) 
                ELSE PERCENTILE_CONT (0.25) WITHIN GROUP (ORDER BY segment_tt_avg ASC) 
            END AS baseline_tt

FROM 		segment_60_tt
LEFT JOIN   congestion.network_segments_highway AS highway USING (segment_id)
GROUP BY 	segment_id, highway.segment_id;
-----------------------------------------------------------------------------------------------------------------------------------------
-- if still missing, find the old baseline and calculate new baseline using the new length

with temp as (
	select distinct o.segment_id as old_seg, n.segment_id as new_seg
	from congestion.network_links_22_2 o
	left join congestion.network_links_23_4 n using (link_dir)
	where n.segment_id > 7133)

select temp.*, 	o.total_length/baseline_tt as spd, baseline_tt, o.total_length as old_length, 
    n.total_length as new_length, n.total_length/(o.total_length/baseline_tt) as new_baseline
from temp
left join congestion.network_segments o on old_seg = segment_id
left join congestion.network_baseline b on old_seg = b.segment_id
left join congestion.network_segments_23_4 n on new_seg = n.segment_id
-----------------------------------------------------------------------------------------------------------------------------------------
-- Backfill for those newly created segments
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
    WHERE 		(dt >= '2017-09-01' AND dt < '2022-08-28') and segment_id > 7056
    
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

-- Backfill monthly data
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
				holiday.dt IS NULL and segment_id > 7056
    
	GROUP BY    segment_id, mth, hr, day_type
	ORDER BY    segment_id, mth, hr, day_type;

-----------------------------------------------------------------------------------------------------------------------------------------
-- !!!!! After moving those segments to the retired table !!!!!!- Check update_segments_retired.sql
-- Finally delete outdated ones from the congestion.network_segments and network_links
DELETE FROM congestion.network_segments
WHERE segment_id IN (
	select segment_id as old_s
	from congestion.network_segments seg
	join (
		select ST_Transform(ST_buffer(ST_Transform(geom, 2952), 50), 4326) as geom from bqu.traffic_signal
		where activationdate >= '2022-04-14') new_signal on ST_intersects(new_signal.geom, seg.geom)
	and segment_id < 7056)

DELETE FROM congestion.network_links_22_2
WHERE segment_id IN (
	select segment_id as old_s
	from congestion.network_segments seg
	join (
		select ST_Transform(ST_buffer(ST_Transform(geom, 2952), 50), 4326) as geom from bqu.traffic_signal
		where activationdate >= '2022-04-14') new_signal on ST_intersects(new_signal.geom, seg.geom)
	and segment_id < 7056);

