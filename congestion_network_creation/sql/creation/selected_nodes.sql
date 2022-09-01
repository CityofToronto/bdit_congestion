-- This mat view stores the geometry, node_id
-- that will be used in creating the congestion network 2.0
-- these are selected here nodes that are intersections and will be used for routing

CREATE MAterialized view congestion.selected_nodes AS				
-- first select the centreline that we are interested
-- aka minor arterial and above

With interested_class AS (
	select geo_id, lf_name, fnode, tnode, fcode_desc, geom, 
	ST_transform(ST_buffer(ST_transform(geom, 2952), 50), 4326) as b_geom -- buffer of 50m for each centreline int and px
	from gis.centreline 
	where fcode_desc in ('Expressway', 
						 'Major Arterial',
						 'Minor Arterial'))					
 -- grab all nodes from here routing street
, interested_nodes as (
	select source as node_id
	from here.routing_streets_21_1
	union all
	select target as node_id
	from here.routing_streets_21_1)
	
-- select intersections that were used more or equal 3 times
-- aka intersections
-- plus end of the road 
, intersection_int as (
	select node_id
	from interested_nodes
	group by node_id
	having count(node_id) >=3 or count(node_id) = 1)

, all_node as (
	select distinct node_id, a.geom
	from intersection_int
	inner join here.routing_nodes_21_1 a using (node_id))

-- Get all the here nodes that are within Expressway, Major arterial,
-- minor arterials and are intersections 
-- this will be used for further routing node selection 
select distinct node_id, all_node.geom
from all_node
inner join interested_class on ST_within(all_node.geom, b_geom); 	


COMMENT ON MATERIALIZED VIEW congestion.selected_nodes
    IS 'HERE nodes that are intersections and are within Expressway, Major and Minor arterial. Created for further nodes selection for routing.';
