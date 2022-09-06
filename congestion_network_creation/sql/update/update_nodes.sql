-- Find new nodes for outdated nodes using nearest neighbour
with retired_nodes AS (
	select distinct node_id, geom 
	from here.routing_nodes_21_1
	inner join (select node_id from congestion.network_nodes
				except 
				select node_id from here.routing_nodes_22_2)a using (node_id))
				
select 	retired_nodes.node_id as old_id, nodes.node_id as new_id, geom, node_geom, dist
from 	retired_nodes
CROSS JOIN LATERAL (SELECT z.node_id,
							z.geom AS node_geom, 
							(ST_transform(retired_nodes.geom, 2952) <-> ST_Transform(z.geom, 2952)) as dist	
					FROM here.routing_nodes_22_2 z
					ORDER BY (retired_nodes.geom <-> z.geom)
					LIMIT 1) nodes;

-- Create a temp table for updates
CREATE TABLE congestion.network_nodes_new AS

SELECT * FROM congestion.network_nodes;

-- Update the temp table with new node ids
with retired_nodes AS (
	select distinct node_id, geom 
	from here.routing_nodes_21_1
	inner join (select node_id from congestion.network_nodes
				except 
				select node_id from here.routing_nodes_22_2)a using (node_id))
                
, prep_update AS (select 	retired_nodes.node_id as old_id, nodes.node_id as new_id, geom, node_geom, dist
from 	retired_nodes
CROSS JOIN LATERAL (SELECT z.node_id,
							z.geom AS node_geom, 
							(ST_transform(retired_nodes.geom, 2952) <-> ST_Transform(z.geom, 2952)) as dist	
					FROM here.routing_nodes_22_2 z
					ORDER BY (retired_nodes.geom <-> z.geom)
					LIMIT 1) nodes)

UPDATE congestion.network_nodes_new
SET node_id = new_id
FROM prep_update
WHERE network_nodes_new.node_id = old_id;

-- Double check that all nodes are in the new map version (22_2)
-- returned 0 rows
select node_id from congestion.network_nodes_new
except 
select node_id from here.routing_nodes_22_2 



