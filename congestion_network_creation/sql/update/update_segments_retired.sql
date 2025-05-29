-- Move retired segments to the retired table

INSERT INTO congestion.network_segments_retired
WITH new_signal AS (
    SELECT ST_Transform(ST_buffer(ST_Transform(geom, 2952), 50), 4326) AS geom
    FROM bqu.traffic_signal -- change to gis schema when ready
    WHERE activationdate >= '2022-04-14'
) -- date of last version's max activatation date

SELECT
    segment_id,
    start_vid,
    end_vid,
    seg.geom,
    total_length,
    highway,
    _s.int_id AS start_int,
    _t.int_id AS end_int,
    _s.px::int AS start_px,
    _t.px::int AS end_px,
    '21_1' AS here_version,
    '20220705' AS centreline_version,
    '2022-09-08'::date AS retired_date,
    'new traffic signal' AS retired_reason,
    NULL::int [] AS replaced_id, -- to be updated later during re-routing new segments
    min(dt) AS valid_from,
    max(dt) AS valid_to

FROM congestion.network_segments AS seg

-- where segments intersects with new traffic signals 
JOIN new_signal ON ST_intersects(new_signal.geom, seg.geom)
INNER JOIN congestion.network_segments_daily USING (segment_id) -- to get valid from to date ranges
-- to get equivalent start px and int_id
INNER JOIN congestion.network_int_px_21_1 AS _s ON start_vid = _s.node_id
-- to get equivalent end px and int_id
INNER JOIN congestion.network_int_px_21_1 AS _t ON end_vid = _t.node_id
WHERE segment_id < 7056 -- only get old segments
GROUP BY
    segment_id,
    start_vid,
    end_vid,
    seg.geom,
    total_length,
    highway,
    start_int,
    end_int,
    start_px,
    end_px,
    here_version,
    centreline_version,
    retired_date, retired_reason, replaced_id;


-- Move individual retired segments to the retired table

INSERT INTO congestion.network_segments_retired
SELECT
    segment_id,
    start_vid,
    end_vid,
    seg.geom,
    total_length,
    highway,
    _s.int_id AS start_int,
    _t.int_id AS end_int,
    _s.px::int AS start_px,
    _t.px::int AS end_px,
    '22_2' AS here_version,
    '20220705' AS centreline_version,
    '2023-03-31'::date AS retired_date,
    'new traffic signal' AS retired_reason,
    ARRAY[7110, 7111] AS replaced_id, -- to be updated later during re-routing new segments
    min(dt) AS valid_from,
    max(dt) AS valid_to

FROM congestion.network_segments AS seg

INNER JOIN congestion.network_segments_daily USING (segment_id) -- to get valid from to date ranges
-- to get equivalent start px and int_id
INNER JOIN congestion.network_int_px_21_1 AS _s ON start_vid = _s.node_id
-- to get equivalent end px and int_id
INNER JOIN congestion.network_int_px_21_1 AS _t ON end_vid = _t.node_id
WHERE segment_id = 1865
GROUP BY
    segment_id,
    start_vid,
    end_vid,
    seg.geom,
    total_length,
    highway,
    start_int,
    end_int,
    start_px,
    end_px,
    here_version,
    centreline_version,
    retired_date, retired_reason, replaced_id;

-- update retired segments tabls with replaced id
UPDATE congestion.network_segments_retired
SET replaced_id = replaced
FROM (
    SELECT
        segment_id,
        array_agg(s) AS replaced
    FROM (
        SELECT
            seg.segment_id AS s,
            retired.segment_id,
            seg.start_vid,
            seg.end_vid,
            retired.start_vid,
            retired.end_vid,
            seg.geom,
            retired.geom
        FROM congestion.network_segments_retired AS retired
        INNER JOIN congestion.network_segments AS seg ON ST_intersects(retired.geom, seg.geom)
        WHERE
            retired_reason = 'new traffic signal'
            AND seg.segment_id > 7056
            AND gis.direction_from_line(retired.geom) = direction
    ) AS a
    GROUP BY segment_id
) AS b
WHERE b.segment_id = network_segments_retired.segment_id