-- Move retired segments to the retired table

insert into congestion.network_segments_retired
with new_signal as (
	select ST_Transform(ST_buffer(ST_Transform(geom, 2952), 50), 4326) as geom 
    from bqu.traffic_signal -- change to gis schema when ready
	where activationdate >= '2022-04-14') -- date of last version's max activatation date

select 	segment_id, start_vid, end_vid, seg.geom, total_length, highway, 
		_s.int_id as start_int, _t.int_id as end_int, 
		_s.px::int start_px, _t.px::int as end_px, 
		'21_1' as here_version, '20220705' as centreline_version, 
		'2022-09-08'::date as retired_date, 'new traffic signal' as retired_reason, 
		null::int[] as replaced_id, -- to be updated later during re-routing new segments
		min(dt) as valid_from, max(dt) as valid_to

from congestion.network_segments seg

JOIN new_signal on ST_intersects(new_signal.geom, seg.geom) -- where segments intersects with new traffic signals 
INNER JOIN congestion.network_segments_daily USING (segment_id) -- to get valid from to date ranges
INNER JOIN congestion.network_int_px_21_1 _s on start_vid = _s.node_id -- to get equivalent start px and int_id
INNER JOIN congestion.network_int_px_21_1 _t on end_vid  = _t.node_id -- to get equivalent end px and int_id
WHERE segment_id  < 7056 -- only get old segments
GROUP BY segment_id, start_vid, end_vid, seg.geom, total_length, highway, start_int, end_int, start_px, end_px, here_version, centreline_version, 
		retired_date, retired_reason, replaced_id;


-- Move individual retired segments to the retired table

insert into congestion.network_segments_retired
select 	segment_id, start_vid, end_vid, seg.geom, total_length, highway, 
		_s.int_id as start_int, _t.int_id as end_int, 
		_s.px::int start_px, _t.px::int as end_px, 
		'22_2' as here_version, '20220705' as centreline_version, 
		'2023-03-31'::date as retired_date, 'new traffic signal' as retired_reason, 
		array[7110,7111] as replaced_id, -- to be updated later during re-routing new segments
		min(dt) as valid_from, max(dt) as valid_to

from congestion.network_segments seg

INNER JOIN congestion.network_segments_daily USING (segment_id) -- to get valid from to date ranges
INNER JOIN congestion.network_int_px_21_1 _s on start_vid = _s.node_id -- to get equivalent start px and int_id
INNER JOIN congestion.network_int_px_21_1 _t on end_vid  = _t.node_id -- to get equivalent end px and int_id
WHERE segment_id  = 1865
GROUP BY segment_id, start_vid, end_vid, seg.geom, total_length, highway, start_int, end_int, start_px, end_px, here_version, centreline_version, 
		retired_date, retired_reason, replaced_id;	 
	 
-- update retired segments tabls with replaced id
update  congestion.network_segments_retired
set replaced_id = replaced
from (select segment_id, array_agg(s) as replaced 
	 from (
	select seg.segment_id s, retired.segment_id , seg.start_vid, seg.end_vid, retired.start_vid, retired.end_vid, seg.geom, retired.geom
	from congestion.network_segments_retired retired 
	inner join congestion.network_segments seg on ST_intersects(retired.geom, seg.geom)
	where retired_reason = 'new traffic signal' and 
	  seg.segment_id > 7056 and 
	  gis.direction_from_line(retired.geom)  = direction)a
	 group by segment_id)b
	 where b.segment_id = network_segments_retired.segment_id		