-- This mat view stores the geometry, centreline intersection (int_id), and traffic signal id (px)
-- that will be used in creating the congestion network 2.0
-- we will than find the here nodes that represents these chosen geometries 

CREATE MATERIALIZED VIEW congestion.selected_intersections AS 
-- first select the centreline that we are interested
-- aka minor arterial and above
With interested_class AS (
	select geo_id, lf_name, fnode, tnode, fcode_desc, geom, 
	ST_transform(ST_buffer(ST_transform(geom, 2952), 10), 4326) as b_geom
	from gis.centreline 
	where fcode_desc in ('Expressway', 
						 'Major Arterial', 
						 'Minor Arterial'))
                         
-- grab all nodes that make up the centrelines we want
, interested_int as (
	select fnode as int_id
	from interested_class
	union all
	select tnode as int_id
	from interested_class)
	
-- select intersections that were used more or equal 3 times
-- aka intersections
-- and 1 time, aka the end of the road
, intersection_int as (
	select int_id
	from interested_int
	group by int_id
	having count(int_id) >=3 or count(int_id) = 1) 
	
-- selection of px with int_ids that doesnt match with our version of intersections
-- which can include midblocks and too updated int_id 
, other_px as (
	select node_id, px, traffic_signal.geom 
	from gis.traffic_signal
    inner join gis.centreline_intersection on node_id = int_id
	where int_id is null)
	
-- select all intersections 
, selected_int as (
-- intersections that are minor arterial and above
	select int_id, null as px
	from intersection_int
	union 
-- intersections that have traffic signals 	
	select node_id, px
	from gis.traffic_signal
	except 
-- except traffic signals that are in other_px cte
	select node_id, px
	from other_px)

-- join the selected int and traffic signals together
, all_int as (
	select inte.int_id, px, inte.geom
	from selected_int
	inner join gis.centreline_intersection inte using (int_id)
	union 
	select null as int_id, px, other_px.geom
	from other_px)

-- selecting only the intersections within a 10m buffer of centreline 
-- mainly to get rid of traffic signals that are not on 
-- the fcode that we want
select distinct int_id, px, all_int.geom
from all_int
inner join interested_class on ST_within(all_int.geom, b_geom); 

COMMENT ON materialized view congestion.selected_intersections IS 'Centreline intersections and traffic signals selected for congestion network routing. Created on 2022-05-17. '










