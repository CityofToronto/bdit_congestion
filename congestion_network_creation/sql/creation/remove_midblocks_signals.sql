Create table congestion.temp_network_segments_24_4 as 
select * FROM congestion.network_segments_24_4;

Create table congestion.temp_network_links_24_4 as 
select * FROM congestion.network_links_24_4;

CREATE TABLE congestion.segments_retired_from_midblock AS 
WITH nodes_to_removed AS (
SELECT node_id FROM congestion.network_nodes_24_4 
EXCEPT
SELECT node_id FROM congestion.network_nodes_24_4_nomidblock
)
-- this generally works really well until some of their directions are not the same
, find_affected_seg as (
select 
	segment_id, 
	node_id, 
	case when start_vid = node_id then null else start_vid end as start_vid,
	case when end_vid = node_id then null else end_vid end as end_vid, 
	dir, 
	geom
from congestion.temp_network_segments_24_4
inner join nodes_to_removed on start_vid = node_id or end_vid = node_id
order by node_id)

-- max start_vid and end_vid cause the other one would be null 
-- first pass does not deal with segments that need to be merged
-- but has different dir, e.g. north and east bound when they 
-- should be both north 
, first_pass AS (
select 
	node_id, 
	ARRAY_AGG(segment_id) AS segment_id, 
	max(start_vid) as start_vid, 
	max(end_vid) as end_vid, 
	dir, 
	ST_linemerge(ST_union(geom)) as geom
from find_affected_seg
group by node_id, dir)
-- deal with the direction issue
, second_pass AS (
select 	
	node_id, 
	ARRAY(SELECT unnest(ARRAY_AGG(segment_id))) AS segment_id, 
	max(start_vid) as start_vid, 
	max(end_vid) as end_vid, 
	ARRAY_AGG(dir) as dir, 
	ST_linemerge(ST_union(geom)) as geom
	from first_pass
	where start_Vid is null or end_vid is null
	group by node_id)

, third_pass as (
	select 
	node_id, segment_id AS segment_ids, 
	start_vid, end_vid, 
	dir, geom 
	from first_pass
	where array_length(segment_id, 1) = 2 and 
	start_Vid is not null and end_vid is not null
	and start_vid != end_vid
	union
	select node_id, segment_id AS segment_ids, 
	start_vid, end_vid, 
	dir[1], geom  from second_pass
	where array_length(segment_id, 1) = 2 and start_Vid is not null and end_vid is not null
	and start_vid != end_vid)
    
-- deal with when we need to get with of
-- two continous nodes
, two_nodes_case as (
SELECT  distinct id, 
max(case when start_vid = ANY(seq_node) then null else start_vid end) as source,
max(case when end_Vid = ANY(seq_node) then null  else end_Vid end) as target,
array_agg(distinct segment_id) as merged_seg, seq_node, array_agg(temp_network_segments_24_4.dir) as dirs, 
ST_union(ST_linemerge(temp_network_segments_24_4.geom)) as geom
from (
select row_number() over() AS id, ARRAY[third_pass.node_id, b.node_id] as seq_node, 
array_cat(third_pass.segment_ids, b.segment_ids) AS merged_segments
from third_pass
inner join third_pass b on (third_pass.segment_ids &&  b.segment_ids) 
AND third_pass.node_id != b.node_id)a
inner join congestion.temp_network_segments_24_4 on segment_id = ANY(merged_segments)
group by id, seq_node)

select row_number() over() + (select max (segment_id) from congestion.temp_network_segments_24_4) 
as new_Segment_id, a.*
from (SELECT third_pass.segment_ids, 
third_pass.start_vid, third_pass.end_vid, 
third_pass.dir, third_pass.geom 
from third_pass
left join two_nodes_case on node_id = ANY(seq_node)
where two_nodes_case.seq_node is null
union 
select merged_seg, source, target, dirs[1], geom from two_nodes_case)a;


insert into congestion.temp_network_links_24_4
select new_segment_id as segment_id, new.start_vid, new.end_vid, 
links.link_dir, links.geom, links.length, NULL AS segment_length
From congestion.temp_network_links_24_4 links
inner join  congestion.segments_retired_from_midblock new
on  segment_id = ANY(segment_ids)
order by new_segment_id; 

delete from congestion.temp_network_links_24_4
where segment_id = ANY(select unnest(segment_ids) from congestion.segments_retired_from_midblock);

-- manually delete the one on allen that should've been kept
delete from congestion.temp_network_links_24_4
where segment_id = 7388;
insert into congestion.temp_network_links_24_4
select * from congestion.network_links_24_4
where segment_id in (1418,
5401)


-- delete merged segments in network_segments
with need_delete as (
select distinct segment_id from  congestion.network_links_24_4
except
select distinct segment_id from  congestion.temp_network_links_24_4)
delete from  congestion.temp_network_segments_24_4
where segment_id in (
select segment_id from need_delete
inner join congestion.network_segments_24_4 using (segment_id))

-- add new merged segments to network_segmentws

with need_update as (
select distinct segment_id from  congestion.temp_network_links_24_4
except
select distinct segment_id from  congestion.network_links_24_4)
insert into congestion.temp_network_segments_24_4
select segment_id,
    start_vid,
    end_vid,
    ST_linemerge(ST_union(geom)) AS geom,
    round(ST_length(ST_transform(ST_linemerge(ST_union(geom)), 2952))::numeric, 2) AS total_length,
	false as highway, -- checked the deleted ones
	gis.direction_from_line(ST_linemerge(ST_union(geom)))
	from congestion.temp_network_links_24_4
inner join need_update using (segment_id)
GROUP BY segment_id, start_vid, end_vid
ORDER BY segment_id, start_vid, end_vid;
