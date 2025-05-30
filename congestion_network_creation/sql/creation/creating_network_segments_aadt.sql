CREATE TABLE congestion.network_segment_aadt AS
-- Centreline that are outdated and will
-- not match to the segments look up table
WITH centreline_missing AS (
    SELECT 
        teps_1.geo_id,
        teps_1.dir_bin,
        st_transform(centreline.shape, 4326) AS geom,
        teps_1.aadt
    FROM covid.teps_aadt_2018 teps_1
    JOIN prj_volume.centreline ON centreline.centreline_id = teps_1.geo_id
    LEFT JOIN gis.centreline_20220705 cent USING (geo_id)
    WHERE cent.geo_id IS NULL), 
-- Find the outdated centreline's new geo_id
-- with spatial join        
new_aadt_cent AS (
    SELECT 
        new_cent.geo_id,
        old_cent.dir_bin,
        avg(old_cent.aadt) AS aadt
    FROM centreline_missing old_cent
    JOIN gis.centreline_20220705 new_cent ON st_intersects(st_transform(st_buffer(st_transform(old_cent.geom, 2952), 5::double precision, 'endcap=flat join=round'::text), 4326), new_cent.geom)
    WHERE st_length(st_transform(st_intersection_deprecated_by_postgis_301(st_transform(st_buffer(st_transform(old_cent.geom, 2952), 5::double precision, 'endcap=flat join=round'::text), 4326), new_cent.geom), 2952)) >= 5::double precision AND (new_cent.fcode_desc::text = ANY (ARRAY['Expressway'::character varying::text, 'Major Arterial'::character varying::text, 'Minor Arterial'::character varying::text, 'Collector'::character varying::text])) AND gis.direction_from_line(old_cent.geom)::text = gis.direction_from_line(new_cent.geom)::text
    GROUP BY new_cent.geo_id, old_cent.dir_bin), 
-- Prepare aadt dataset with up to date centreline_id aka geo_id
prep_aadt AS (
    (
    SELECT 
        teps_1.geo_id,
        teps_1.dir_bin,
        teps_1.aadt
    FROM covid.teps_aadt_2018 teps_1
    EXCEPT
    SELECT 
        centreline_missing.geo_id,
        centreline_missing.dir_bin,
        centreline_missing.aadt
    FROM centreline_missing
    ) UNION
    SELECT 
        new_aadt_cent.geo_id,
        new_aadt_cent.dir_bin,
        new_aadt_cent.aadt
    FROM new_aadt_cent), 
-- Join to segment <-> centreline lookup table    
prep_segment_centreline_1 AS (
    SELECT 
        segment_centreline_lookup.segment_id,
        unnest(segment_centreline_lookup.geo_id_set) AS geo_id,
        segment_centreline_lookup.start_int,
        segment_centreline_lookup.end_int,
        network_segments.direction,
        array_length(segment_centreline_lookup.geo_id_set, 1) AS num_length,
        CASE
            WHEN network_segments.direction = ANY (ARRAY['Northbound'::text, 'Eastbound'::text]) THEN 1
            ELSE '-1'::integer
        END AS dir_bin
    FROM congestion.segment_centreline_lookup
    JOIN congestion.network_segments USING (segment_id)), 
-- Add dir_bin     
prep_segment_centreline AS (
    SELECT 
        a.segment_id,
        a.geo_id,
        a.start_int,
        a.end_int,
        a.direction,
        a.num_length,
        COALESCE(
            CASE
                WHEN centreline_20220705.oneway_dir <> 0 THEN centreline_20220705.oneway_dir
            ELSE NULL::integer
            END, a.dir_bin) AS dir_bin1,
        a.dir_bin
    FROM prep_segment_centreline_1 a
    JOIN gis.centreline_20220705 USING (geo_id))

-- Calulate average aadt for each segment_id
SELECT prep_segment_centreline.segment_id,
    prep_segment_centreline.start_int,
    prep_segment_centreline.end_int,
    prep_segment_centreline.direction,
    avg(teps.aadt) AS aadt,
    count(teps.geo_id) AS num_bin,
    prep_segment_centreline.num_length
   FROM prep_aadt teps
     JOIN prep_segment_centreline USING (geo_id, dir_bin)
  GROUP BY prep_segment_centreline.segment_id, prep_segment_centreline.start_int, prep_segment_centreline.end_int, prep_segment_centreline.direction, prep_segment_centreline.num_length

------------------------------------------------------
-- Below contains the update sql used to manually 
-- assign aadt for segments (64 segment_id)
-- using adjacent streets with aadt
------------------------------------------------------
-- Queens Park (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT aadt AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id = 5424)adjacent
WHERE segment_id IN (2248, 2765, 2258) AND aadt IS NULL; 
-- Queens Park (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT aadt AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id = 4007)adjacent
WHERE segment_id IN (5415, 5405) AND aadt IS NULL;
------------------------------------------------------
-- Doris Ave (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT aadt AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id = 7033)adjacent
WHERE segment_id IN (7031) AND aadt IS NULL;
-- Doris Ave (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT aadt AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id = 7034)adjacent
WHERE segment_id IN (7032) AND aadt IS NULL;
------------------------------------------------------
-- Islington Ave (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT aadt AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id = 869)adjacent
WHERE segment_id IN (662) AND aadt IS NULL;
-- Islington Ave (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT aadt AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id = 870)adjacent
WHERE segment_id IN (4100) AND aadt IS NULL;
------------------------------------------------------
-- Weston Rd (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT aadt AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id = 1139)adjacent
WHERE segment_id IN (1141) AND aadt IS NULL;
-- Weston Rd (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT aadt AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id = 4364)adjacent
WHERE segment_id IN (4375) AND aadt IS NULL;
------------------------------------------------------
-- Old Weston Rd (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT aadt AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id = 1146)adjacent
WHERE segment_id IN (1145) AND aadt IS NULL;
-- Old Weston Rd (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT aadt AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id = 4383)adjacent
WHERE segment_id IN (4363) AND aadt IS NULL;
------------------------------------------------------
-- Birchmount Rd (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (5687, 634))adjacent
WHERE segment_id IN (2520, 2519) AND aadt IS NULL;
-- Birchmount Rd (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (5689, 2827))adjacent
WHERE segment_id IN (5688, 2826) AND aadt IS NULL;
------------------------------------------------------
-- Lawrence Ave E (EB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (5963))adjacent
WHERE segment_id IN (628) AND aadt IS NULL;
-- Lawrence Ave E (WB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (5962))adjacent
WHERE segment_id IN (5964) AND aadt IS NULL;
------------------------------------------------------
-- Military Trail (EB)
UPDATE congestion.network_segments_aadt
SET aadt = (8273.079922 + 4249.573986)/2 -- using centreline geo_id 8105565, 14658974
WHERE segment_id IN (6216, 2776) AND aadt IS NULL;
-- Military Trail (WB)
UPDATE congestion.network_segments_aadt
SET aadt = (5928.105506 + 5556.104289)/2 -- using centreline geo_id 8105565, 14658974
WHERE segment_id IN (515, 6215) AND aadt IS NULL;
------------------------------------------------------
-- Queen's Park 
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (7103))adjacent
WHERE segment_id IN (6476) AND aadt IS NULL;
------------------------------------------------------
-- Spadina Ave (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (1685, 1688))adjacent
WHERE segment_id IN (4867) AND aadt IS NULL;
-- Spadina Ave (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (1686, 4865))adjacent
WHERE segment_id IN (6395) AND aadt IS NULL;
------------------------------------------------------
-- Front St W (EB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (1648))adjacent
WHERE segment_id IN (1645) AND aadt IS NULL;
-- Front St W (WB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (1670))adjacent
WHERE segment_id IN (4846) AND aadt IS NULL;
------------------------------------------------------
-- Bremner Blvd (EB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (1654))adjacent
WHERE segment_id IN (6388) AND aadt IS NULL;
-- Bremner Blvd (WB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (1655))adjacent
WHERE segment_id IN (6390) AND aadt IS NULL;
------------------------------------------------------
-- Lower Sherbourne St (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (419))adjacent
WHERE segment_id IN (1862) AND aadt IS NULL;
------------------------------------------------------
-- Don Valley Parkway S (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (5188))adjacent
WHERE segment_id IN (16) AND aadt IS NULL;
-- Don Valley Parkway S (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (2939))adjacent
WHERE segment_id IN (3356) AND aadt IS NULL;
------------------------------------------------------
-- The West Mall (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3527))adjacent
WHERE segment_id IN (6249) AND aadt IS NULL;
-- The West Mall (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (131))adjacent
WHERE segment_id IN (3196) AND aadt IS NULL;
------------------------------------------------------
-- Eglinton Ave W (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3726))adjacent
WHERE segment_id IN (259) AND aadt IS NULL;
-- Eglinton Ave W (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3727))adjacent
WHERE segment_id IN (3725) AND aadt IS NULL;
------------------------------------------------------
-- Eglinton Ave W (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3726))adjacent
WHERE segment_id IN (259) AND aadt IS NULL;
-- Eglinton Ave W (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3727))adjacent
WHERE segment_id IN (3725) AND aadt IS NULL;
------------------------------------------------------
-- Hoskin Ave (EB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (4931, 4934))adjacent
WHERE segment_id IN (1740) AND aadt IS NULL;
------------------------------------------------------
-- Lake Shore Blvd W (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3735))adjacent
WHERE segment_id IN (167) AND aadt IS NULL;
-- Lake Shore Blvd W (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3989))adjacent
WHERE segment_id IN (168) AND aadt IS NULL;

------------------------------------------------------
-- Lake Shore Blvd W (NB) (south of park lawn)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (342, 167))adjacent
WHERE segment_id IN (3570) AND aadt IS NULL;
-- Lake Shore Blvd W (SB) (south of park lawn)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (165, 168))adjacent
WHERE segment_id IN (3572) AND aadt IS NULL;
-- Lake Shore Blvd W (SB) (south of park lawn) (on the north ish side)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (164, 168))adjacent
WHERE segment_id IN (3571) AND aadt IS NULL;
------------------------------------------------------
-- Six Point
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (123, 833))adjacent
WHERE segment_id IN (1, 130) AND aadt IS NULL;
------------------------------------------------------
-- Highway 27 S (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (6117, 3760))adjacent
WHERE segment_id IN (280) AND aadt IS NULL;
-- Highway 27 S (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3771, 279))adjacent
WHERE segment_id IN (670) AND aadt IS NULL;
------------------------------------------------------
-- Renforth Dr (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3729, 3178))adjacent
WHERE segment_id IN (3176) AND aadt IS NULL;
-- Renforth Dr (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3730, 6286))adjacent
WHERE segment_id IN (3179) AND aadt IS NULL;
------------------------------------------------------
-- Renforth Dr (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3729, 3178))adjacent
WHERE segment_id IN (3176) AND aadt IS NULL;
-- Renforth Dr (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3730, 6286))adjacent
WHERE segment_id IN (3179) AND aadt IS NULL;
------------------------------------------------------
-- Highway 427 N (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3551, 147, 3180))adjacent
WHERE segment_id IN (146) AND aadt IS NULL;
------------------------------------------------------
-- Highway 427 X N (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (3537))adjacent
WHERE segment_id IN (134) AND aadt IS NULL;
------------------------------------------------------
-- Vaughan Rd (EB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (387))adjacent
WHERE segment_id IN (3351) AND aadt IS NULL;
------------------------------------------------------
-- Spadina (Roundabout)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (4867, 1686))adjacent
WHERE segment_id IN (1687) AND aadt IS NULL;
-- Spadina (Roundabout)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (6395, 1685))adjacent
WHERE segment_id IN (1684) AND aadt IS NULL;
------------------------------------------------------
-- Neilson Rd (NB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (2886))adjacent
WHERE segment_id IN (2883) AND aadt IS NULL;
-- Neilson Rd (SB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (6065))adjacent
WHERE segment_id IN (6070) AND aadt IS NULL;
------------------------------------------------------
-- McNicoll Ave (EB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (6057))adjacent
WHERE segment_id IN (7053) AND aadt IS NULL;
-- McNicoll Ave (WB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (2872))adjacent
WHERE segment_id IN (7054) AND aadt IS NULL;
------------------------------------------------------
-- Morningside Ave (EB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (7053, 2893))adjacent
WHERE segment_id IN (7047,7050) AND aadt IS NULL;
-- Morningside Ave (WB)
UPDATE congestion.network_segments_aadt
SET aadt = adjacent.adjacent_aadt
FROM (SELECT avg(aadt) AS adjacent_aadt 
	 FROM congestion.network_segments_aadt
	 WHERE segment_id IN (7054, 2891))adjacent
WHERE segment_id IN (7048, 7049) AND aadt IS NULL;
------------------------------------------------------
-- Pape Ave (little ramp)
UPDATE congestion.network_segments_aadt
SET aadt = 12683.45061
WHERE segment_id IN (3489) AND aadt IS NULL;
------------------------------------------------------
-- Parliament St (little ramp)
UPDATE congestion.network_segments_aadt
SET aadt = 13995.46246
WHERE segment_id IN (1925) AND aadt IS NULL;

