CREATE MATERIALIZED VIEW tts.tts06_neighbourhoods AS 
 WITH zones AS (
         SELECT zones_tts06.gta06 AS zone_number,
            st_area(st_makevalid(st_transform(zones_tts06.geom, 26717))) AS area_m2,
            st_makevalid(st_transform(zones_tts06.geom, 26717)) AS geom
           FROM tts.zones_tts06
        ), prop AS (
         SELECT a_1.area_s_cd,
		a_1.area_name,
            b_1.zone_number,
            st_area(st_intersection(st_transform(a_1.geom, 26717), b_1.geom)) / b_1.area_m2 AS proportion
           FROM gis.to_neighbourhood a_1
             JOIN zones b_1 ON (st_area(st_intersection(st_transform(a_1.geom, 26717), b_1.geom)) / b_1.area_m2) > 0.05::double precision
        )
 SELECT a.area_s_cd,
	a.area_name,
	a.zone_number,
	a.proportion / b.total_prop AS allocation
   FROM prop a
     JOIN ( SELECT prop.zone_number,
            sum(prop.proportion) AS total_prop
           FROM prop
          GROUP BY prop.zone_number) b USING (zone_number)
  ORDER BY a.zone_number
WITH DATA;