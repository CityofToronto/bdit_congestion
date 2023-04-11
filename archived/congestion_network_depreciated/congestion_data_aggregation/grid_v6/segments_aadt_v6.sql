-- Purpose: tagging segment_id to one or more centreline_ids in gis.centreline to transfer AADT estimates for TTI weighting purposes. (Quick and dirty as oppose to perfect match)
-- Issue 61 in bdit_data_analysis for version 5


-- First find the ones that got new links 
-- 13187 rows
WITH new_link as (
	select v6.segment_id, v6.link_dir
	from congestion.segment_links_v5 v5
	right join congestion.segment_links_v6_21_1 v6 using (segment_id, link_dir)
	where v5.link_dir is null)
	
-- Find the ones that got matched with sharedstreets
-- Matched: 11700/13187 (88.7%)
, conflated AS (
	SELECT 		DISTINCT segment_id,
						  link_dir,
						  geo_id,
						 CASE WHEN dir::text = ANY (ARRAY['Northbound'::character varying::text, 'Eastbound'::character varying::text]) THEN 1
                    		ELSE '-1'::integer end as  dir_bin
	FROM 		gis_shared_streets.centreline_dir_here_21_1_180430
	INNER JOIN 	new_link using (link_dir)

        )

-- Match the rest of them using spatial join
-- First create a 10m buffer geom for centreline in Minor Arterial and Up
, buff AS (
 	SELECT 		vol.geo_id,
            	st_transform(st_buffer(st_transform(cen.geom, 2952), 10::double precision, 'endcap=flat join=round'::text), 4326) AS buff_geom,
           	 	cen.geom,
            	vol.dir_bin
           FROM covid.teps_aadt_2018 vol
           JOIN gis.centreline cen USING (geo_id)
           WHERE cen.fcode_desc::text = ANY (ARRAY['Expressway'::text, 'Expressway Ramp'::text, 'Major Arterial'::text, 'Major Arterial Ramp'::text, 'Minor Arterial'::text])
)
-- Match missing links with buffered centreline geometries 
-- Using ST_within and line direction
-- 566 links got matched
, buff_joined AS (
	SELECT 		seg.segment_id,
    			seg.link_dir,
				buff.geo_id,
				buff.dir_bin

    FROM 		congestion.segment_links_v6_21_1 seg
    LEFT JOIN 	conflated using (link_dir)
	INNER JOIN  new_link using (link_dir)
	INNER JOIN  here.routing_streets_21_1 here using (link_dir)
	JOIN 		buff ON st_within(here.geom, buff.buff_geom) AND
						CASE
							WHEN gis.direction_from_line(here.geom)::text = ANY (ARRAY['Northbound'::text, 'Eastbound'::text]) THEN 1
							ELSE '-1'::integer
						END = buff.dir_bin
    WHERE 		conflated.link_dir is null)
	
-- Estimate AADT for each segment based on matched links
, matched_aadt AS(
	select segment_id, avg(aadt)
	from (select distinct segment_id, geo_id, dir_bin from conflated
		  UNION 
		  select distinct segment_id, geo_id, dir_bin  from buff_joined)matched_segs
	inner join covid.teps_aadt_2018 using (geo_id, dir_bin)
	group by segment_id)

select distinct segment_id from congestion.segments_v6
left join matched_aadt using (segment_id)
left join (select v6.segment_id, v6.link_dir
			from congestion.segment_links_v5 v5
			inner join congestion.segment_links_v6_21_1 v6 using (segment_id, link_dir))a using (segment_id)
where matched_aadt.segment_id is null and a.segment_id is null 



-- There are only 72 segments with no match
-- Considering they are mostly short links and 
-- Their aadt would be similar to neighbouring segments
-- We will use neighbouring segment's aadt as missing links aadt

	select link_dir, geom
	from new_link
	LEFT join conflated using (link_dir)
	LEFT JOIN buff_joined using (link_dir)
	inner join  here.routing_streets_21_1 here using (link_dir)
	where conflated.link_dir is null and buff_joined.link_dir is null
