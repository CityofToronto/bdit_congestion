-- Find new nodes for outdated nodes using nearest neighbour
WITH a AS (
    SELECT node_id FROM congestion.network_nodes
    EXCEPT
    SELECT node_id FROM here.routing_nodes_22_2
),

retired_nodes AS (
    SELECT DISTINCT
        node_id,
        geom
    FROM here.routing_nodes_21_1
    INNER JOIN a USING (node_id)
)

SELECT
    retired_nodes.node_id AS old_id,
    nodes.node_id AS new_id,
    geom,
    node_geom,
    dist
FROM retired_nodes
CROSS JOIN LATERAL (SELECT
    z.node_id,
    z.geom AS node_geom,
    (ST_transform(retired_nodes.geom, 2952) <-> ST_Transform(z.geom, 2952)) AS dist
FROM here.routing_nodes_22_2 AS z
ORDER BY (retired_nodes.geom <-> z.geom)
LIMIT 1) AS nodes;

-- Create a temp table for updates
CREATE TABLE congestion.network_nodes_new AS

SELECT * FROM congestion.network_nodes;

-- Update the temp table with new node ids
WITH retired_nodes AS (
    SELECT DISTINCT
        node_id,
        geom
    FROM here.routing_nodes_21_1
    INNER JOIN (
        SELECT node_id FROM congestion.network_nodes
        EXCEPT
        SELECT node_id FROM here.routing_nodes_22_2
    ) AS a USING (node_id)
),

prep_update AS (
    SELECT
        retired_nodes.node_id AS old_id,
        nodes.node_id AS new_id,
        geom,
        node_geom,
        dist
    FROM retired_nodes
    CROSS JOIN LATERAL (SELECT
        z.node_id,
        z.geom AS node_geom,
        (ST_transform(retired_nodes.geom, 2952) <-> ST_Transform(z.geom, 2952)) AS dist
    FROM here.routing_nodes_22_2 AS z
    ORDER BY (retired_nodes.geom <-> z.geom)
    LIMIT 1) AS nodes
)

UPDATE congestion.network_nodes_new
SET node_id = new_id
FROM prep_update
WHERE network_nodes_new.node_id = old_id;

-- Double check that all nodes are in the new map version (22_2)
-- returned 0 rows
SELECT node_id FROM congestion.network_nodes_new
EXCEPT
SELECT node_id FROM here.routing_nodes_22_2



