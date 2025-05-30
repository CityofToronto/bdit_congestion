-- Final table with here routing_streets_21_1 for routing

CREATE TABLE congestion.network_routing_grid AS

-- Used last cleaned version to quickly eliminate most unwanted links in the current 
-- version, e.g. local roads and collectors
with last_version as (
     select * from here.routing_streets_18_3
     left join  congestion.routing_grid using (link_dir)
     where routing_grid.link_dir is null )

select * 
from here.routing_streets_21_1
left join temp using (link_dir)
where  temp.link_dir is null 


COMMENT ON TABLE congestion.network_routing_grid IS '''HERE network layer for routing, created by eliminating unwanted link_dirs from previous cleaned version 
                                                       This table got further manual cleaning in QGIS. 
                                                    '''