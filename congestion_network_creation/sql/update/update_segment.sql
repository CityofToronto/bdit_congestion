-- Find segments that needs to be updated due to new traffic signals
with new_signal as (
	select ST_Transform(ST_buffer(ST_Transform(geom, 2952), 50), 4326) as geom from bqu.traffic_signal
	where activationdate >= '2022-04-17')

select seg.*
from congestion.network_segments seg
join new_signal on ST_intersects(new_signal.geom, seg.geom)