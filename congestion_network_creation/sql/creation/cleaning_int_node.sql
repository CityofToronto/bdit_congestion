-- update int_id <-> node_id layer with node geom

update congestion.network_int_px_21_1
set node_geom = geom
from congestion.network_nodes  a
where network_int_px_21_1_updated.node_id = a.node_id


-- Checked if node_id was tagged to more than 1 int_id /px
-- returned 9 
-- manually fixed them till no node_ids are being tagged
-- to more than 1 int_id/px
select node_id from 
congestion.network_int_px_21_1
group by node_id
having count(1) >1


-- Check if any node_id was not tagged to any int_id
-- somehow theres 25 rows....how did they sneak in here!
-- manually fixed them, and deleted ones that were the start and end vid of
-- retired segments 
select * from congestion.network_nodes
left join congestion.network_int_px_21_1 a using (node_id)
where a.node_id is null 


-- Check if there are int_id with px 
-- and those int_id have entries without px
-- returned 8, updated table manually 
with temp as (
select distinct int_id, px from congestion.network_int_px_21_1
where int_id is not null and px is not null )
select * from temp
inner join congestion.network_int_px_21_1 a using (int_id)
where a.px is null 

