-- This mat view stores the geometry, node_id
-- that will be used in creating the congestion network 2.0
-- these are selected here nodes that are intersections and will be used for routing

CREATE MATERIALIZED VIEW congestion.selected_nodes AS
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
        -- buffer of 50m for each centreline int and px
        ST_transform(ST_buffer(ST_transform(geom, 2952), 50), 4326) AS b_geom
    FROM gis.centreline
    WHERE fcode_desc IN (
        'Expressway',
        'Major Arterial',
        'Minor Arterial'
    )
),

-- grab all nodes from here routing street
interested_nodes AS (
    SELECT source AS node_id
    FROM here.routing_streets_21_1
    UNION ALL
    SELECT target AS node_id
    FROM here.routing_streets_21_1
),

-- select intersections that were used more or equal 3 times
-- aka intersections
-- plus end of the road 
intersection_int AS (
    SELECT node_id
    FROM interested_nodes
    GROUP BY node_id
    HAVING count(node_id) >= 3 OR count(node_id) = 1
),

all_node AS (
    SELECT DISTINCT
        node_id,
        a.geom
    FROM intersection_int
    INNER JOIN here.routing_nodes_21_1 AS a USING (node_id)
)

-- Get all the here nodes that are within Expressway, Major arterial,
-- minor arterials and are intersections 
-- this will be used for further routing node selection 
SELECT DISTINCT
    node_id,
    all_node.geom
FROM all_node
INNER JOIN interested_class ON ST_within(all_node.geom, b_geom);


COMMENT ON MATERIALIZED VIEW congestion.selected_nodes
IS 'HERE nodes that are intersections and are within Expressway, Major and Minor arterial. Created for further nodes selection for routing.';
