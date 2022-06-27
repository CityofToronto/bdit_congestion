create table congestion.network_segments as 

select  segment_id, 
        start_vid, 
        end_vid,
        ST_linemerge(ST_union(geom)) as geom, 
        round(ST_length(ST_transform(ST_linemerge(ST_union(geom)), 2952))::numeric, 2) as total_length

from congestion.network_links_21_1
group by segment_id, start_vid, end_vid
order by segment_id, start_vid, end_vid;

COMMENT ON TABLE congestion.network_segments IS 
'Containing all the segments in the congestion network.';