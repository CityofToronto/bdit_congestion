-- Create initial matches between intersection layer and network_nodes
-- Very inital match! 
CREATE TABLE congestion.network_int_px_24_4 AS 
SELECT 
    node_id, 
    intersection_id, 
    nodes.geom as node_geom, 
    cip.geom as int_geom, 
    null::text as px, -- holder, will update layer
    intersection_desc, 
    highest_order_feature, 
    road_name_class, 
    degree, 
    centreline_ids
FROM congestion.network_nodes_24_4_nomidblocks nodes
LEFT JOIN gis_core.intersection_classification_temp cip ON true
WHERE st_dwithin(st_transform(cip.geom, 2952), st_transform(nodes.geom, 2952), 50::double precision); -- 50m buffer
------------------------------------
-- Manual checks in QGIS
------------------------------------
-- Manual add matches 
INSERT INTO congestion.network_int_px_24_4(node_id, intersection_id, node_geom, int_geom, px)
SELECT distinct node_id, intersection_id, nodes.geom as node_geom, 
cip.geom as int_geom, null::text as px
--, intersection_desc, 
--highest_order_feature, road_name_class, degree, centreline_ids
From congestion.network_nodes_24_4_nomidblock nodes
left join gis_core.intersection_latest cip
on true
where st_dwithin(st_transform(cip.geom, 2952), 
st_transform(nodes.geom, 2952), 200::double precision) -- usually its the highway ones, so increase buffer to ensure matching
and node_id in (30326251) and intersection_id = 13451503;

-- Delete wrong matches
DELETE from congestion.network_int_px_24_4
where intersection_id in (13453379) and node_id = 30326251;

------------------------------------
-- Update other information like intersection name and px
------------------------------------
-- Update all intersection related information after the matches are finalized
UPDATE congestion.network_int_px_24_4
set intersection_desc = a.intersection_desc,
    highest_order_feature = a.highest_order_feature,
    road_name_class = a.road_name_class,
    degree = a.degree,
    centreline_ids = a.centreline_ids
from gis_core.intersection_classification_temp a
where a.intersection_id = network_int_px_24_4.intersection_id

-- Update px using move's px int matches
UPDATE congestion.network_int_px_24_4
set px = a.px
from traffic.traffic_signal a
where a.intersection_id = network_int_px_24_4.intersection_id