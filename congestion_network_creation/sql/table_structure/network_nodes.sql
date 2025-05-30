CREATE TABLE IF NOT EXISTS congestion.network_nodes
(
    node_id double precision,
    geom geometry(Point,4326)
)

ALTER TABLE congestion.network_nodes
    OWNER to congestion_admins;

COMMENT ON TABLE congestion.network_nodes
    IS '''HERE node layer for routing, created by buffering selected centreline intersections/px and 
          finding selected nodes within that buffer. 
           This table got further manual cleaning in QGIS. 
                                            ''';