-- Creating segment and centreline lookup, including the one that routed through px
CREATE TABLE congestion.segment_centreline AS 

select * from congestion.segments_centreline_routed_21_1_missing
union all
select array[segment_id], start_vid, end_vid, start_int, end_int, array_agg, geom 
		from congestion.segments_centreline_routed_21_1
        
        
-- Creating a temp table that get the main street name for each segment sets        
CREATE TABLE congestion.temp_street_name_segments AS 

WITH prep as (
select segment_set, start_vid, end_vid, start_int, end_int, unnest(geo_id_set) as geo_id, geom
from congestion.segment_centreline)

select segment_set, start_vid, end_vid, start_int, end_int, array_agg(distinct lf_name) as st_name, array_agg(geo_id) as geo_id_set, 
		array_length(array_agg(distinct lf_name), 1), prep.geom 
from prep
inner join gis.centreline_20220705 using (geo_id)
group by segment_set, start_vid, end_vid, start_int, end_int, prep.geom     


-- Final table that includes main, from and to names for each segment sets 
CREATE TABLE congestion.segment_centreline_name AS 
-- excluding planning boundary 
WITH ints AS (

    select 		distinct on (int_id) int_id, linear_name_full_from , linear_name_full_to
    from 		gis.centreline_intersection_20220705
    where not 	linear_name_full_from = 'Planning Boundary' and not linear_name_full_to = 'Planning Boundary'
    order by 	int_id, linear_name_full_from , linear_name_full_to)


select 		segment_set, 
			start_int, 
			end_int, 
			st_name, 
			geo_id_set,
			case 	when array_length = 1 then st_name[1]
		 			when array_length = 2 then st_name[1]||' and '||st_name[2]
		 			when array_length = 3 then st_name[1]||' and '||st_name[2]||' and '||st_name[3] end as main_street, -- making a list into one name 
			coalesce(	case when s.linear_name_full_from = ANY(st_name) then null 
					 			else s.linear_name_full_from end, 
                		case when s.linear_name_full_to = ANY(st_name) then null  
					 			else s.linear_name_full_to end) 
									as start_int_name, 
			coalesce(	case when e.linear_name_full_from = ANY(st_name) then null  
					 			else e.linear_name_full_from end, 
                		case when e.linear_name_full_to = ANY(st_name) then null  
					 			else e.linear_name_full_to end) 
									as end_int_name, 
			seg.geom
from 		congestion.temp_street_name_segments seg
left join 	ints s on s.int_id = start_int
left join  	ints e on e.int_id = end_int
order by 	segment_set


update congestion.segment_centreline_name
set end_int_name = main_street
where end_int = 13470663;
update congestion.segment_centreline_name
set end_int_name = 'International Blvd'
where end_int = 13462898;
update congestion.segment_centreline_name
set start_int_name = 'International Blvd'
where start_int = 13462898;
update congestion.segment_centreline_name
set start_int_name = 'Bartor Rd'
where start_int = 13452927;
update congestion.segment_centreline_name
set end_int_name = 'Bartor Rd'
where end_int = 13452927;
update congestion.segment_centreline_name
set end_int_name = 'Parkside Drive'
where end_int = 14177831;
update congestion.segment_centreline_name
set start_int_name = 'Parkside Drive'
where start_int = 14177831;
update congestion.segment_centreline_name
set end_int_name = 'Don Mills Rd W'
where end_int = 13445233;
update congestion.segment_centreline_name
set start_int_name = 'Don Mills Rd W'
where start_int = 13442737; -- need px 
update congestion.segment_centreline_name
set start_int_name = 'Townsend Rd'
where start_int = 14228218;
update congestion.segment_centreline_name
set end_int_name = 'Lake Shore Blvd W'
where end_int = 13469447;
update congestion.segment_centreline_name
set end_int_name = 'Fairview Mall Dr'
where end_int = 13446011;
update congestion.segment_centreline_name
set start_int_name = 'Fairview Mall Dr'
where start_int = 13446011;
update congestion.segment_centreline_name
set end_int_name = 'Michael Power Pl'
where end_int = 13467775;
update congestion.segment_centreline_name
set start_int_name = 'Michael Power Pl'
where start_int = 13467775;
update congestion.segment_centreline_name
set end_int_name = 'University Ave'
where end_int = 13467453;
update congestion.segment_centreline_name
set start_int_name = 'University Ave'
where start_int = 13467453;
update congestion.segment_centreline_name
set end_int_name = 'Parliament St'
where end_int = 14025083;
update congestion.segment_centreline_name
set start_int_name = 'Parliament St'
where start_int = 14025083;
update congestion.segment_centreline_name
set end_int_name = 'Shadlock St'
where end_int = 14303972;
update congestion.segment_centreline_name
set start_int_name = 'Shadlock St'
where start_int = 14303972;
update congestion.segment_centreline_name
set end_int_name = 'Ninth Line'
where end_int = 14228258;
update congestion.segment_centreline_name
set start_int_name = 'Ninth Line'
where start_int = 14228258;
update congestion.segment_centreline_name
set end_int_name = 'Waggoners Wells Ln'
where end_int = 14303945;
update congestion.segment_centreline_name
set start_int_name = 'Waggoners Wells Ln'
where start_int = 14303945;
update congestion.segment_centreline_name
set end_int_name = 'Pape Ave'
where end_int = 13458569;
update congestion.segment_centreline_name
set start_int_name = 'Pape Ave'
where start_int = 13458569;
update congestion.segment_centreline_name
set end_int_name = 'Marydale Ave'
where end_int = 14228233;
update congestion.segment_centreline_name
set start_int_name = 'Marydale Ave'
where start_int = 14228233;
update congestion.segment_centreline_name
set end_int_name = main_street
where end_int = 13470663;
update congestion.segment_centreline_name
set start_int_name = main_street
where start_int = 13470663;
update congestion.segment_centreline_name
set end_int_name = 'Townsend Rd'
where end_int = 14228218;
update congestion.segment_centreline_name
set end_int_name = 'York & Durham Line'
where end_int = 13441579;
update congestion.segment_centreline_name
set start_int_name = 'York & Durham Line'
where start_int = 13441579;
update congestion.segment_centreline_name
set end_int_name = 'Dundas St W'
where end_int = 13467982;
update congestion.segment_centreline_name
set start_int_name = 'Dundas St W'
where start_int = 13467982;
update congestion.segment_centreline_name
set start_int_name = main_street
where start_int = 13458029;
update congestion.segment_centreline_name
set end_int_name = main_street
where end_int = 13458029;
update congestion.segment_centreline_name
set end_int_name = main_street
where end_int = 13464117;
update congestion.segment_centreline_name
set start_int_name = main_street
where start_int = 13464117;
update congestion.segment_centreline_name
set end_int_name = '530m South of Finch Ave E'
where end_int = 13442737;
update congestion.segment_centreline_name
set start_int_name = '530m South of Finch Ave E'
where start_int = 13442737;
update congestion.segment_centreline_name
set end_int_name = '260m West of Chetta Pl'
where end_int = 13459423;
update congestion.segment_centreline_name
set start_int_name = '260m West of Chetta Pl'
where start_int = 13459423;
update congestion.segment_centreline_name
set end_int_name = '75m Noth of Mack Ave'
where end_int = 13457877;
update congestion.segment_centreline_name
set start_int_name = '75m Noth of Mack Ave'
where start_int = 13457877;
update congestion.segment_centreline_name
set start_int_name = 'Don Mills Rd W'
where start_int = 13445233;
update congestion.segment_centreline_name
set end_int_name = 'Cherry St'
where end_int = 13466977;
update congestion.segment_centreline_name
set start_int_name = 'St Clair Ave W'
where start_int = 13461960 and segment_set = ARRAY[74::bigint];
update congestion.segment_centreline_name
set end_int_name = 'St Clair Ave W'
where end_int = 13461960 and segment_set = ARRAY[3459::bigint];



-- Find name using traffic signal
with temp as (
	select distinct start_int, px
	from congestion.segment_centreline_name
	inner join congestion.network_int_px_21_1 on int_id = start_int
	where start_int_name is null
	union 
	select  distinct  end_int, px from congestion.segment_centreline_name
	inner join congestion.network_int_px_21_1 on int_id = end_int
	where end_int_name is null)
select 	start_int, * from gis.traffic_signal
inner join temp using (px)

-- Fix incorrectly routed centrelines
update  congestion.segment_centreline
set  end_int = 13461960 , geo_id_set = ARRAY[1140787,14003540,14003539]
where segment_set = ARRAY[3459::bigint]

update  congestion.segment_centreline_name
set  end_int = 13461960 , geo_id_set = ARRAY[1140787,14003540,14003539]
where segment_set = ARRAY[3459::bigint]

update  congestion.segment_centreline
set  start_int = 13461960 , geo_id_set = ARRAY[1140787,14003540,14003539]
where segment_set = ARRAY[74::bigint]

update  congestion.segment_centreline_name
set  start_int = 13461960 , geo_id_set = ARRAY[1140787,14003540,14003539]
where segment_set = ARRAY[74::bigint]

-- Fixing the geoms 
update  congestion.segment_centreline
set  geom = un
from (select ST_union(ST_linemerge(geom)) as un from gis.centreline_20220705 
	  where geo_id in (1140787,14003539,14003540)
	  ) a
where segment_set = ARRAY[3459::bigint]

update  congestion.segment_centreline_name
set  geom = un
from (select ST_union(ST_linemerge(geom)) as un from gis.centreline_20220705 
	  where geo_id in (1140787,14003539,14003540)
	  ) a
where segment_set = ARRAY[3459::bigint]

update  congestion.segment_centreline
set  geom = un
from (select ST_union(ST_linemerge(geom)) as un from gis.centreline_20220705 
	  where geo_id in (1140787,14003539,14003540)
	  ) a
where segment_set = ARRAY[74::bigint]

update  congestion.segment_centreline_name
set  geom = un
from (select ST_union(ST_linemerge(geom)) as un from gis.centreline_20220705 
	  where geo_id in (1140787,14003539,14003540)
	  ) a
where segment_set = ARRAY[74::bigint]