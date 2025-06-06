CREATE TABLE congestion.network_baseline AS
--SELECT 6456
--Query returned successfully in 13 min 49 secs.
-- Aggregate travel time for each link up to 30 mins 
WITH link_60_tt AS (
    SELECT
        segment_id,
        link_dir,
        datetime_bin(tx, 60) AS datetime_bin,
        avg(links.length * 0.001 / mean * 3600) AS link_tt,
        links.length

    FROM here.ta
    INNER JOIN congestion.network_links_21_1 AS links USING (link_dir)
    LEFT JOIN ref.holiday AS hol ON hol.dt = tx::date
    WHERE
        -- Only aggregating free flow tt using 2019 data 
        tx >= '2019-01-01 00:00:00' AND tx < '2020-01-01 00:00:00'
        AND hol.dt IS NULL -- exclude holiday dates
        AND date_part('isodow'::text, tx)::integer < 6 -- include only weekdays 
        AND confidence >= 30 -- only use high confidence data

    GROUP BY segment_id, link_dir, datetime_bin, links.length
),

segment_60_tt AS (
    SELECT
        segment_id,
        datetime_bin,
        total_length / (sum(link_60_tt.length) / sum(link_60_tt.link_tt)) AS segment_tt_avg

    FROM link_60_tt
    INNER JOIN congestion.network_segments USING (segment_id)

    GROUP BY datetime_bin, segment_id, total_length
    HAVING sum(link_60_tt.length) >= (total_length * 0.8)
)-- where at least 80% of links have data

SELECT
    segment_id,
    PERCENTILE_CONT(0.10) WITHIN GROUP (
        ORDER BY segment_tt_avg ASC
    ) AS baseline_10pct,
    PERCENTILE_CONT(0.15) WITHIN GROUP (
        ORDER BY segment_tt_avg ASC
    ) AS baseline_15pct,
    PERCENTILE_CONT(0.20) WITHIN GROUP (
        ORDER BY segment_tt_avg ASC
    ) AS baseline_20pct,
    PERCENTILE_CONT(0.25) WITHIN GROUP (
        ORDER BY segment_tt_avg ASC
    ) AS baseline_25pct

FROM segment_60_tt
WHERE datetime_bin::time >= '07:00:00' AND datetime_bin::time < '21:00:00'
GROUP BY segment_id;


---------------------------------------------------------------------------------------------------------------
-- Aggregating baseline for segments that has no baseline tt 
-- because of map version issues (More details in #58)
-- Includes segment_id (7019,7020,7033,7034,2569,2193,128,129,130,230,504,569,642,1207,1519,2442,2443,2480,2482,2572,
-- 2649,2838,2839,3062,3078,3157,3348,3522,4419,4716,4763,5452,5626,5637,5649,5650,
-- 5653,5697,5698,5714,5721,5778,5779,5790,5814,5820,5950,6358,6454,6497)

-- Create temporary table 
-- route missing segments with old map version (19_4_tc)
CREATE TABLE congestion.temp_missing_segment AS
WITH missing AS (
    SELECT * FROM congestion.network_segments
    LEFT JOIN congestion.network_baseline USING (segment_id)
    WHERE network_baseline.segment_id IS NULL
),

new_route AS (
    SELECT
        routing_grid.link_dir,
        missing.start_vid,
        missing.end_vid,
        missing.segment_id,
        ST_length(ST_Transform(routing_grid.geom, 2952)) AS length
    FROM missing
    ,
        LATERAL pgr_dijkstra(
            'SELECT id, source::int, target::int,
						   st_length(st_transform(geom, 2952)) as cost
						   FROM here.routing_streets_19_4_tc routing_grid
						   ',
            start_vid, end_vid
        )
    INNER JOIN here.routing_streets_19_4_tc AS routing_grid ON id = edge
)


-- Aggregate baseline tt using _old here.ta data
-- which is based on map version 19_4_tc and 
-- insert into the congestion.network_baseline table
INSERT INTO congestion.network_baseline

WITH link_60_tt AS (
    SELECT
        links.segment_id,
        link_dir,
        datetime_bin(tx, 60) AS datetime_bin,
        avg(links.length * 0.001 / mean * 3600) AS link_tt,
        links.length
    -- Union all the _old version of ta		
    FROM (
        SELECT * FROM here.ta_201901_old
        UNION ALL
        SELECT * FROM here.ta_201902_old
        UNION ALL
        SELECT * FROM here.ta_201903_old
        UNION ALL
        SELECT * FROM here.ta_201904_old
        UNION ALL
        SELECT * FROM here.ta_201905_old
        UNION ALL
        SELECT * FROM here.ta_201906_old
        UNION ALL
        SELECT * FROM here.ta_201907_old
        UNION ALL
        SELECT * FROM here.ta_201908_old
        UNION ALL
        SELECT * FROM here.ta_201909_old
        UNION ALL
        SELECT * FROM here.ta_201910_old
        UNION ALL
        SELECT * FROM here.ta_201911_old
        UNION ALL
        SELECT * FROM here.ta_201912_old
    ) AS ta

    INNER JOIN congestion.temp_missing_segment AS links USING (link_dir)
    LEFT JOIN ref.holiday AS hol ON hol.dt = tx::date
    WHERE
        hol.dt IS NULL -- exclude holiday dates
        AND date_part('isodow'::text, tx)::integer < 6 -- include only weekdays 
        AND confidence >= 30 -- only use high confidence data

    GROUP BY links.segment_id, link_dir, datetime_bin, links.length
),

segment_60_tt AS (
    SELECT
        segment_id,
        datetime_bin,
        total_length / (sum(link_60_tt.length) / sum(link_60_tt.link_tt)) AS segment_tt_avg

    FROM link_60_tt
    INNER JOIN congestion.network_segments USING (segment_id)

    GROUP BY datetime_bin, segment_id, total_length
    HAVING sum(link_60_tt.length) >= (total_length * 0.8)
)-- where at least 80% of links have data

SELECT
    segment_id,
    PERCENTILE_CONT(0.10) WITHIN GROUP (
        ORDER BY segment_tt_avg ASC
    ) AS baseline_10pct,
    PERCENTILE_CONT(0.15) WITHIN GROUP (
        ORDER BY segment_tt_avg ASC
    ) AS baseline_15pct,
    PERCENTILE_CONT(0.20) WITHIN GROUP (
        ORDER BY segment_tt_avg ASC
    ) AS baseline_20pct,
    PERCENTILE_CONT(0.25) WITHIN GROUP (
        ORDER BY segment_tt_avg ASC
    ) AS baseline_25pct

FROM segment_60_tt
WHERE datetime_bin::time >= '07:00:00' AND datetime_bin::time < '21:00:00'
GROUP BY segment_id;
-------------------------------------------------------------------------------------------------------------------
-- For the 4 segments at Six point where the streets were drawn a little differently
-- See #58 for more details

WITH missing (segment_id, start_vid, end_vid) AS (
    VALUES (129, 30335582, 30335581),
    (3522, 30335581, 30335582),
    (128, 968661376, 30335589),
    (130, 30335589, 968661376)
),

temp_missing_segment AS (
    SELECT
        results.*,
        routing_grid.id,
        routing_grid.link_dir,
        routing_grid.geom,
        missing.start_vid,
        missing.end_vid,
        missing.segment_id,
        ST_length(ST_Transform(routing_grid.geom, 2952)) AS length
    FROM missing
    ,
        LATERAL pgr_dijkstra(
            'SELECT id, source::int, target::int,
						   st_length(st_transform(geom, 2952)) as cost
						   FROM here.routing_streets_19_4_tc routing_grid
						   ',
            start_vid, end_vid
        ) AS results
    INNER JOIN here.routing_streets_19_4_tc AS routing_grid ON id = edge
),

link_60_tt AS (
    SELECT
        links.segment_id,
        link_dir,
        datetime_bin(tx, 60) AS datetime_bin,
        avg(links.length * 0.001 / mean * 3600) AS link_tt,
        links.length

    FROM (
        SELECT * FROM here.ta_201901_old
        UNION ALL
        SELECT * FROM here.ta_201902_old
        UNION ALL
        SELECT * FROM here.ta_201903_old
        UNION ALL
        SELECT * FROM here.ta_201904_old
        UNION ALL
        SELECT * FROM here.ta_201905_old
        UNION ALL
        SELECT * FROM here.ta_201906_old
        UNION ALL
        SELECT * FROM here.ta_201907_old
        UNION ALL
        SELECT * FROM here.ta_201908_old
        UNION ALL
        SELECT * FROM here.ta_201909_old
        UNION ALL
        SELECT * FROM here.ta_201910_old
        UNION ALL
        SELECT * FROM here.ta_201911_old
        UNION ALL
        SELECT * FROM here.ta_201912_old
    ) AS ta

    INNER JOIN temp_missing_segment AS links USING (link_dir)
    LEFT JOIN ref.holiday AS hol ON hol.dt = tx::date
    WHERE
        hol.dt IS NULL -- exclude holiday dates
        AND date_part('isodow'::text, tx)::integer < 6 -- include only weekdays 
        AND confidence >= 30 -- only use high confidence data

    GROUP BY links.segment_id, link_dir, datetime_bin, links.length
),

segment_60_tt AS (
    SELECT
        segment_id,
        datetime_bin,
        total_length / (sum(link_60_tt.length) / sum(link_60_tt.link_tt)) AS segment_tt_avg

    FROM link_60_tt
    INNER JOIN congestion.network_segments USING (segment_id)

    GROUP BY datetime_bin, segment_id, total_length
-- HAVING 		sum(link_60_tt.length) >= (total_length * 0.8) 
)

INSERT INTO congestion.network_baseline
SELECT
    segment_id,
    PERCENTILE_CONT(0.10) WITHIN GROUP (
        ORDER BY segment_tt_avg ASC
    ) AS baseline_10pct,
    PERCENTILE_CONT(0.15) WITHIN GROUP (
        ORDER BY segment_tt_avg ASC
    ) AS baseline_15pct,
    PERCENTILE_CONT(0.20) WITHIN GROUP (
        ORDER BY segment_tt_avg ASC
    ) AS baseline_20pct,
    PERCENTILE_CONT(0.25) WITHIN GROUP (
        ORDER BY segment_tt_avg ASC
    ) AS baseline_25pct

FROM segment_60_tt
WHERE datetime_bin::time >= '07:00:00' AND datetime_bin::time < '21:00:00'
GROUP BY segment_id;