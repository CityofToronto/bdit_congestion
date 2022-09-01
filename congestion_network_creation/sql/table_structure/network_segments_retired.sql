-- Table structure for network_segments_retired
-- which contains all retired segments 

CREATE TABLE congestion.network_segments_retired  (
segment_id bigint, 
start_vid bigint, 
end_vid bigint,
geom geometry,
total_length numeric, 
highway boolean,
start_int bigint,
end_int bigint,
start_px bigint,
end_px bigint,
here_version text,
centreline_version text,
retired_date date,
retired_reason text, 
replaced_id int[],
valid_from date,
valid_to date);


COMMENT ON TABLE congestion.network_segments_retired IS 
'Containing all retired segments in the congestion network.';


insert into congestion.network_segments_retired
select segment_id, start_vid, end_vid, geom, total_length, highway, f.int_id as start_int, 
t.int_id as end_int, f.px as start_px, t.px as end_px, '21_1' as here_version, null as centreline_version, 
'2022-07-20' as retired_date, 'outdated' as retired_reason, null as replaced_id, null as valid_from, null as valid_to

from congestion.network_segments a
left join congestion.network_int_px_21_1_updated f on start_vid = node_id
left join congestion.network_int_px_21_1_updated t on end_vid = t.node_id
where segment_id in (3588, 3589)
