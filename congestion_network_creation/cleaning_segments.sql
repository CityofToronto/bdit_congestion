-- Contains the sql I used for cleaning segments manually


-- Delete nodes from network_nodes
delete from congestion.network_nodes
where node_id in (30438528)


-- Insert missing nodes to network nodes
insert into congestion.network_nodes
select distinct node_id, geom
from here.routing_nodes_21_1
where node_id in (30363402)	


-- Insert re-route result to network_routing_results
WITH intersections as(
	SELECT array_agg(node_id::int) AS ints
	FROM congestion.network_nodes xsections
	where node_id in (30438508,30438566)
), results AS(
	SELECT results.*, routing_grid.id, routing_grid.geom
	FROM intersections
	, LATERAL pgr_dijkstra('SELECT id, source::int, target::int,
						   st_length(st_transform(geom, 2952)) as cost
						   FROM here.routing_streets_21_1 routing_grid
						   ',
				ints, ints) results
	INNER JOIN here.routing_streets_21_1 routing_grid ON id = edge
)
insert into congestion.network_routing_results
SELECT null  as segment_id, start_vid, end_vid, array_agg(id order by path_seq) as link_set, array_agg(cost order by path_seq) as length_set, st_linemerge(st_union(s.geom)) as geom, sum(cost) as length
FROM (select distinct path_seq, start_vid, end_vid, edge, node, cost, agg_cost, id, geom  from results)s
LEFT OUTER JOIN congestion.network_nodes ON node = node_id AND node != start_vid
WHERE not (start_vid = 30363407 and end_vid =30363400) and not (start_vid = 30363400 and end_vid =30363407)
GROUP BY start_vid, end_vid
HAVING COUNT(node_id) =0
order by start_vid, end_vid

-- Update network_routing_results with new segment_id
-- Cause reroute segment has NULL as id
update congestion.network_routing_results
set segment_id = uid
from (
select row_number() over() as uid, start_vid, end_vid, link_set, length_set, geom, length
from congestion.network_routing_results)a
where a.start_vid = network_routing_results.start_vid and a.end_vid = network_routing_results.end_vid


-- Find link_dirs that were used more than once
with temp as (select segment_id, start_vid, end_vid, unnest(link_set) as link_uid
from congestion.network_routing_results)

, dups as (
	select count(segment_id), link_dir, link_uid, geom
	from temp
	inner join here.routing_streets_21_1 on id = link_uid
	group by link_dir, geom,link_uid
	having count(segment_id) >  1)

select * from temp
inner join dups using (link_uid)