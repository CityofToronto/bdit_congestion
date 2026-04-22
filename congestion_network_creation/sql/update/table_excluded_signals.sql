-- Initial Table creation, for updating this layer, see this function
-- congestion.insert_excluded_px(px_input TEXT, ver_id TEXT, centreline_ver TEXT)

--Add signals that are not included in the current layer
create table congestion.excluded_signals AS 
SELECT traffic_signal.*, NULL::double precision AS dist, NULL::int as closest_int, NULL::boolean as in_network
FROM gis.traffic_signal
LEFT JOIN (select px::int from congestion.temp_int_px_nodes_25_1 
		   union 
		   select px::int from  congestion.network_int_px_24_4 )a on traffic_signal.px::int = a.px
WHERE a.px is null;

-- set in_network as true if in 20m buffer of congestion network
update congestion.excluded_signals
set in_network = true
where px in (
select px from congestion.excluded_signals a
inner join congestion.temp_network_segments_24_4 seg
on ST_DWithin(ST_Transform(seg.geom, 2952), ST_Transform(a.geom, 2952), 25));

-- update the others one as false
update congestion.excluded_signals
set in_network = false
where in_network is null;

-- update the intersection_id and distance for each signal
update congestion.excluded_signals
set dist = distance, 
closest_int = intersection_id
from (
SELECT ints.px, intersection_id, distance FROM congestion.excluded_signals signals
CROSS JOIN LATERAL ( 
SELECT px,signals.geom as px_geom, intersection_id, ints.geom  as int_geom, 
ST_TRAnsform(signals.geom, 2952)<-> ST_TRAnsform(ints.geom, 2952) as distance
FROM gis_core.intersection_latest ints 
ORDER BY ST_TRAnsform(signals.geom, 2952)<-> ST_TRAnsform(ints.geom, 2952)
limit 1) AS ints )a
where excluded_signals.px = a.px

-- manually update these signals are that listed as in_network but actually arent
update congestion.excluded_signals
set in_network = false
where px in (
'1559',
'2650',
'0299',
'2402')