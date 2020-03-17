-- highway segments for v5
with highway as (
                         select routing_grid.* from congestion.routing_grid
                         join here_gis.streets_att_18_3 on link_id =  (left(id::Text, -1))::numeric
                         where st_name = 'LAKE SHORE BLVD E' or st_name = 'LAKE SHORE BLVD W' or st_name = 'DON VALLEY PKWY' or st_name = 'HWY-404'or st_name ILIKE '%401%' or st_name = 'GARDINER EXPY')
                        
                         select distinct segment_id from congestion.segment_links_v5 
                         inner join highway using (link_dir))