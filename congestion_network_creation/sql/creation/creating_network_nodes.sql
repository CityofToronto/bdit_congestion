-- Final here nodes table for routing

CREATE TABLE congestion.network_nodes AS

-- Use 50m buffer of selected centreline intersections
WITH cent_nodes AS (
    SELECT
        int_id,
        px,
        ST_transform(ST_buffer(ST_transform(geom, 2952), 50), 4326) AS b_geom
    FROM congestion.selected_intersections
)

-- To find the selected here nodes that are within that buffer
SELECT
    node_id,
    geom
FROM congestion.selected_nodes
INNER JOIN cent_nodes ON ST_within(geom, b_geom);

COMMENT ON TABLE congestion.network_nodes IS '''HERE node layer for routing, created by buffering selected centreline intersections/px and 
                                                finding selected nodes within that buffer. 
                                                This table got further manual cleaning in QGIS. 
                                            '''