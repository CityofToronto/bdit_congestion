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
