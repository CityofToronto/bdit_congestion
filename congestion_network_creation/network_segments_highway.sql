-- initial pass of highway selections

CREATE TABLE congestion.network_segments_highway as 

with temp as (
select a.* from congestion.network_links_21_1 a
  inner join here.routing_streets_21_1 using (link_dir)
  inner join here_gis.streets_att_21_1 using (link_id)
  where st_name LIKE 'HWY-409' or st_name 
  LIKE 'HWY-401%' or  
  st_name LIKE  'GARDINER EXPY' or st_name LIKE 'HWY-427%' or st_name LIKE 'DON VALLEY%' 
  or st_name LIKE 'HWY-404' or st_name LIKE 'ALLEN %' or st_name LIKE 'HWY-27'
  or st_name LIKE 'HWY-400' or st_name LIKE 'HWY-2A' or st_name LIKE 'HWY-401')
  select distinct segment_id, total_length, geom from 
  congestion.network_segments a
  inner join temp using (segment_id);
  
COMMENT ON TABLE congestion.network_segments_highway IS 
'A lookup table for the highways in the congestion network.';