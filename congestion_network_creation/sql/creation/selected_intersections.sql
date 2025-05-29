-- This mat view stores the geometry, centreline intersection (int_id), and traffic signal id (px)
-- that will be used in creating the congestion network 2.0
-- we will than find the here nodes that represents these chosen geometries 

CREATE MATERIALIZED VIEW congestion.selected_intersections AS
-- first select the centreline that we are interested
-- aka minor arterial and above
WITH interested_class AS (
    SELECT
        geo_id,
        lf_name,
        fnode,
        tnode,
        fcode_desc,
        geom,
        ST_transform(ST_buffer(ST_transform(geom, 2952), 10), 4326) AS b_geom
    FROM gis.centreline_20220705
    WHERE fcode_desc IN (
        'Expressway',
        'Major Arterial',
        'Minor Arterial'
    )
),

-- grab all nodes that make up the centrelines we want
interested_int AS (
    SELECT fnode AS int_id
    FROM interested_class
    UNION ALL
    SELECT tnode AS int_id
    FROM interested_class
),

-- select intersections that were used more or equal 3 times
-- aka intersections
-- and 1 time, aka the end of the road
intersection_int AS (
    SELECT int_id
    FROM interested_int
    GROUP BY int_id
    HAVING count(int_id) >= 3 OR count(int_id) = 1
),

-- selection of px with int_ids that doesnt match with our version of intersections
-- which can include midblocks and too updated int_id 
other_px AS (
    SELECT
        node_id,
        px,
        traffic_signal.geom
    FROM gis.traffic_signal
    LEFT JOIN gis.centreline_intersection_20220705 ON node_id = int_id
    WHERE int_id IS NULL
),

-- select all intersections 
selected_int AS (
-- intersections that are minor arterial and above
-- and their equivalent px if exist 
    SELECT
        int_id,
        px
    FROM intersection_int
    LEFT JOIN gis.traffic_signal ON node_id = int_id
    UNION
    -- traffic signals with int_ids that are 
    -- in the current centreline intersection version
    (
        SELECT
            node_id,
            px
        FROM gis.traffic_signal
        EXCEPT
        SELECT
            node_id,
            px
        FROM other_px
    )
),

-- join the selected int and other traffic signals together
all_int AS (
    SELECT
        inte.int_id,
        px,
        inte.geom
    FROM selected_int
    INNER JOIN gis.centreline_intersection_20220705 AS inte USING (int_id)
    UNION ALL
    SELECT
        NULL AS int_id,
        px,
        other_px.geom
    FROM other_px
)

-- selecting only the intersections within a 10m buffer of centreline 
-- mainly to get rid of traffic signals that are not on 
-- the fcode that we want
SELECT DISTINCT
    int_id,
    px,
    all_int.geom
FROM all_int
INNER JOIN interested_class ON ST_within(all_int.geom, b_geom);

COMMENT ON MATERIALIZED VIEW congestion.selected_intersections IS 'Centreline intersections and traffic signals selected for congestion network routing. Created on 2022-05-17. '










