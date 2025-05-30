-- Lookup table

CREATE TABLE congestion.segment_centreline_lookup AS

with temp as (
	select row_number() over() as uid, a.*, main_street, start_int_name, end_int_name 
	from congestion.segment_centreline a
	left join congestion.segment_centreline_name using (segment_set) )
, unnest_seg AS (
	select uid, unnest(segment_set) as segment_id , start_vid, end_vid, start_int, end_int, geo_id_set, length as cent_length 
	from temp)
		
, seg_info AS (
	select uid, sum(total_length) as segment_set_length, unnest_seg.start_vid, unnest_seg.end_vid, start_int, end_int, geo_id_set, cent_length 
	from unnest_seg
	inner join congestion.network_segments using (segment_id)
	group by uid, unnest_seg.start_vid, unnest_seg.end_vid, start_int, end_int, geo_id_set, cent_length)

select uid, segment_id, start_vid, end_vid, 
		seg_info.start_int, seg_info.end_int, 
		seg_info.geo_id_set, segment_set_length, seg_info.cent_length
from unnest_seg
inner join seg_info using (uid,start_vid, end_vid) 


COMMENT ON MATERIALIZED VIEW congestion.segment_centreline_lookup
    IS 'A Lookup Table between segment_ids in the congestion network and the geo_id in centreline ';
