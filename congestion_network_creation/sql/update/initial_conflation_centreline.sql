
CREATE TABLE congestion.contracted_result_24_4 as
WITH results as (
SELECT
	id,
	(source||contracted_vertices||target)::bigint[] as contracted_ver,
	array_length((source||contracted_vertices||target)::bigint[], 1) as num_contracted_ver,
	source,
	target
	FROM pgr_contraction(
  '
  SELECT centreline_id as id, source, target,  cost_length as cost
  from gis_core.routing_centreline_directional_higher_rc_20260119

'::text,
  ARRAY[2], --linear contraction only, no deadend
   forbidden_vertices := (SELECT array_agg(distinct intersection_id) AS node_ids 
    FROM congestion.congestion_nodes_lookup_24_4)) as a)

select distinct on (segment_id) segment_id, a.node_id as from_node, b.node_id as to_node, contracted_ver, num_contracted_ver, source, target
from results
left join congestion.congestion_nodes_lookup_24_4 a on source = a.intersection_id
left join congestion.congestion_nodes_lookup_24_4 b on target = b.intersection_id
left join congestion.congestion_segments_24_4 on  a.node_id  = start_vid and b.node_id = end_vid
where segment_id is not null
order by segment_id, num_contracted_ver;

-- Create index for speed
CREATE INDEX source_uid on  congestion.contracted_result_24_4 USING btree
    (source);
CREATE INDEX target_uid on  congestion.contracted_result_24_4 USING btree
    (target);	

------------------------------------
-- Create centreline matches using 
-- contracted results
------------------------------------
-- First add segments where there is only 1 centreline is within source and target
CREATE TABLE congestion.temp_contract_24_4 as
select distinct segment_id, a.node_id as from_node, b.node_id as to_node, null::bigint[] as contracted_ver,
null::int as num_contracted_ver, a.intersection_id as from_int, b.intersection_id as to_int,
array[centreline_id] as ids, cent.geom
from congestion.temp_congestion_segments_24_4
left join congestion.congestion_nodes_lookup_24_4 a on start_vid = a.node_id
left join congestion.congestion_nodes_lookup_24_4 b on end_vid = b.node_id
left join gis_core.routing_centreline_directional_higher_rc_20260119 cent on source = a.intersection_id and target = b.intersection_id
where id is not null;

-- Use contracted results to route centreline
-- Only use edges that has contracted nodes 
insert into congestion.temp_contract_24_4
SELECT distinct segment_id, from_node, to_node, contracted_ver,
num_contracted_ver, source as from_int,
target as to_int, links as ids, geom
FROM (SELECT contracted_result_24_4.* FROM congestion.contracted_result_24_4
	  LEFT JOIN congestion.temp_contract_24_4 using (segment_id)
	  WHERE temp_contract_24_4.segment_id IS  NULL) results
left join lateral congestion.route_contracted_path(
    results.source,  -- start node
    results.target,  -- end node
    contracted_ver -- only allow edges that has nodes that got contracted
)a on true
where segment_id is not null and segment_id not in (7568, 7583, 2781, 2894); 

------------------------------------
-- Create centreline matches for  
-- segments WITHOUT contracted results
-- using pgr_trsp
------------------------------------
-- Route the non-highway ones first
WITH seg_to_route AS (
    SELECT segment_id, start_vid, end_vid
    FROM congestion.temp_congestion_segments_24_4
	where highway != true -- exclude highway for now, its very messy, e.g. garidiner + lakeshore
    EXCEPT
    SELECT segment_id, from_node, to_node
    FROM congestion.temp_contract_24_4
	order by segment_id
),

temp as (
select segment_id, s.node_id as start_hnode, s.intersection_id as start_vid, e.node_id as end_hnode,
e.intersection_id as end_vid
from seg_to_route
left join congestion.congestion_nodes_lookup_24_4 s on start_vid = node_id
left join congestion.congestion_nodes_lookup_24_4 e on end_vid = e.node_id),
results AS (
    SELECT  t.start_vid, t.end_vid, edge, cost, agg_cost, seq, segment_id, node, path_seq, start_hnode, end_hnode
    FROM temp t
    CROSS JOIN LATERAL pgr_trsp(
        $$
        SELECT
            id,
            source::int,
            target::int,
            cost_length::int AS cost
        FROM gis_core.routing_centreline_directional_higher_rc_20260119
        $$,
		$$SELECT path, cost FROM gis_core.centreline_routing_restrictions_higher_rc_20260119 $$,
        t.start_vid, t.end_vid,true
    ) AS route
),

agg AS (
    SELECT
        segment_id,
		start_hnode,
		end_hnode,
        d.start_vid AS start_node,
        d.end_vid   AS end_node,
        array_agg(r.centreline_id ORDER BY path_seq) AS link_arr,
        ST_LineMerge(ST_Union(r.geom))               AS geom_out
    FROM results d
    JOIN gis_core.routing_centreline_directional_higher_rc_20260119 r
      ON d.edge = r.id
    GROUP BY segment_id, d.start_vid, d.end_vid, d.start_hnode, d.end_hnode
)
insert into congestion.temp_contract_24_4
SELECT distinct on(segment_id) segment_id, start_hnode, end_hnode,
null as contracted_ver,
null as num_contracted_ver, start_node as from_int, end_node as to_int,
link_arr, geom_out
FROM agg
order by segment_id, ST_Length(geom_out) ;-- take only the shortest one

-- Route the highway ones and put them into another table
-- for easier inspection and quicker reruns
create table congestion.temp_1_to_1_check_highway as
WITH seg_to_route AS (
    SELECT segment_id, start_vid, end_vid
    FROM congestion.temp_congestion_segments_24_4
	where highway is true
    EXCEPT
    SELECT segment_id, from_node, to_node
    FROM congestion.temp_contract_24_4
),

temp as (
select distinct segment_id, s.intersection_id as start_vid, e.intersection_id as end_vid
from seg_to_route
left join congestion.congestion_nodes_lookup_24_4 s on start_vid = node_id
left join congestion.congestion_nodes_lookup_24_4 e on end_vid = e.node_id
),
results AS (
    SELECT t.start_vid, t.end_vid, edge, cost, agg_cost, seq, segment_id, node, path_seq
    FROM temp t
    CROSS JOIN LATERAL pgr_trsp(
        $$
        SELECT
            id,
            source::int,
            target::int,
            cost_length::int AS cost
        FROM gis_core.routing_centreline_directional_higher_rc_20260119
        $$,
		$$SELECT path, cost FROM gis_core.centreline_routing_restrictions_higher_rc_20260119 $$,
        t.start_vid, t.end_vid,true
    ) AS route
),

agg AS (
    SELECT
        segment_id,
        d.start_vid AS start_node,
        d.end_vid   AS end_node,
        array_agg(r.centreline_id ORDER BY path_seq) AS link_arr,
        ST_LineMerge(ST_Union(r.geom))               AS geom_out
    FROM results d
    JOIN gis_core.routing_centreline_directional_higher_rc_20260119 r
      ON d.edge = r.id
    GROUP BY segment_id, d.start_vid, d.end_vid
)

SELECT distinct on(segment_id) segment_id, start_node, end_node, link_arr, geom_out
FROM agg
order by segment_id, ST_Length(geom_out);   -- take only the shortest one

------------------------------------
-- Creates the look up table based on
-- the above results
------------------------------------
drop table congestion.temp_centreline_lookup_24_4;
create table congestion.temp_centreline_lookup_24_4 AS
select segment_id, from_int, to_int, ids, geom
from congestion.temp_contract_24_4;

insert into congestion.temp_centreline_lookup_24_4
select segment_id, start_node, end_node, link_arr::bigint[], geom_out
from congestion.temp_1_to_1_check_highway;

------------------------------------
-- Manual fixes
-- For where shortest path is not the correct path
-- Affects only two segments
------------------------------------
delete from  congestion.temp_centreline_lookup_24_4
where segment_id in (141, 154);

WITH seg_to_route AS (
    SELECT segment_id, start_vid, end_vid
    FROM congestion.temp_congestion_segments_24_4
	where segment_id in (141, 154)
),
temp as (
select segment_id, s.intersection_id as start_vid, e.intersection_id as end_vid
from seg_to_route
left join congestion.congestion_nodes_lookup_24_4 s on start_vid = node_id
left join congestion.congestion_nodes_lookup_24_4 e on end_vid = e.node_id),
results AS (
    SELECT t.start_vid, t.end_vid, edge, cost, agg_cost, seq, segment_id, node, path_seq
    FROM temp t
    CROSS JOIN LATERAL pgr_trsp(
        $$
        SELECT
            id,
            source::int,
            target::int,
            cost_length::int AS cost
        FROM gis_core.routing_centreline_directional_higher_rc_20260119
		WHERE centreline_id not in (14257277, 20061350, 30066680)
        $$,
		$$SELECT path, cost FROM gis_core.centreline_routing_restrictions_higher_rc_20260119 $$,
        t.start_vid, t.end_vid,true
    ) AS route
),

agg AS (
    SELECT
        segment_id,
        d.start_vid AS start_node,
        d.end_vid   AS end_node,
        array_agg(r.centreline_id ORDER BY path_seq) AS link_arr,
        ST_LineMerge(ST_Union(r.geom))               AS geom_out
    FROM results d
    JOIN gis_core.routing_centreline_directional_higher_rc_20260119 r
      ON d.edge = r.id
    GROUP BY segment_id, d.start_vid, d.end_vid
)

insert into congestion.temp_centreline_lookup_24_4
SELECT distinct on(segment_id) segment_id, start_node, end_node, link_arr, geom_out
FROM agg
order by segment_id, ST_Length(geom_out);   -- take only the shortest one

------------------------------------
-- Manual fixes
-- https://github.com/CityofToronto/bdit_congestion/issues/91#issuecomment-3899378987
------------------------------------
-- Manually delete the 2 incorrectly matched ones
delete from congestion.temp_centreline_lookup_24_4
where segment_id = 146 and ids = ARRAY[909848];
delete from congestion.temp_centreline_lookup_24_4
where segment_id = 6484 and ids = ARRAY[60023450];

-- Comment out the following for debugging
drop table congestion.contracted_result_24_4;
drop table congestion.temp_contract_24_4;
drop table congestion.temp_1_to_1_check_highway;
