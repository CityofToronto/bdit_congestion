-- initial pass of highway selections

UPDATE congestion.network_segments
SET highway = TRUE
FROM (
    SELECT DISTINCT segment_id AS seg
    FROM congestion.network_links_21_1
    INNER JOIN here.routing_streets_21_1 USING (link_dir)
    INNER JOIN here_gis.streets_att_21_1 USING (link_id)
    WHERE
        st_name LIKE 'HWY-409'
        OR st_name LIKE 'HWY-401%'
        OR st_name LIKE 'GARDINER EXPY'
        OR st_name LIKE 'HWY-427%'
        OR st_name LIKE 'DON VALLEY%'
        OR st_name LIKE 'HWY-404'
        OR st_name LIKE 'ALLEN %'
        OR st_name LIKE 'HWY-27'
        OR st_name LIKE 'HWY-400'
        OR st_name LIKE 'HWY-2A'
        OR st_name LIKE 'HWY-401'
) AS hwy
WHERE segment_id = seg