CREATE OR REPLACE VIEW tts.tts_neighbourhoods_vehicles_ampk_origins AS 
 SELECT COALESCE(b.area_name, 'Outside City Boundaries'::character varying) AS area_orig,
    round(sum(a.total::double precision * COALESCE(b.allocation, 1::double precision))) AS total_trips
   FROM tts.tts_ampk_drivers a
     LEFT JOIN tts.tts06_neighbourhoods b ON a.gta06_orig = b.zone_number
     LEFT JOIN tts.tts06_neighbourhoods c ON a.gta06_dest = c.zone_number
  WHERE (b.area_name IS NOT NULL OR c.area_name IS NOT NULL)
  GROUP BY b.area_name
  ORDER BY regexp_replace(b.area_name, '\D','','g')::numeric