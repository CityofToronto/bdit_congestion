CREATE MATERIALIZED VIEW  congestion.corridor_routing
with centreline as (
 SELECT geo_id, lf_name, geom, fcode_desc FROM gis.centreline 
 where fcode_desc in ('Major Arterial'))
 ,here as (							
 select * from here_gis.streets_18_3 
 join here_gis.streets_att_18_3 using (link_id))
 , include as (select link_id, st_name, here.geom 
 from centreline
 join here on UPPER(lf_name) = st_name)
 , selected as ( 
 select link_id, st_name, geom  from include
 union
 SELECT link_id, st_name, geom from here_gis.streets_18_3 
 JOIN here_gis.streets_att_18_3 using (link_id)
 WHERE link_id in ( -- missing links on richmond and adelaide without st_name
792823582,792823583,792823584,792823585,946785256,946785258,754977367,1055933477,943853841,1055933478,948895950,1063427385,1063427386,1055930549,133800550) or 
	 st_name IN ('MT PLEASANT RD','ALLEN RD', 'PRINCE EDWARD VIAD', 'LEASIDE BRG', 'BROWNS LINE', 'N QUEEN ST','S KINGSWAY', 'BRIDLETOWNE CIR', 'SEWELLS RD', 'HWY-27') 
	)
select routing.* from here.routing_streets_18_3 routing
join selected using (link_id)


