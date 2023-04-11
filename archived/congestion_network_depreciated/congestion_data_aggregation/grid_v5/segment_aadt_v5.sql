-- Issue 61 in Data analysis repo

-- View: covid.segment_aadt

-- DROP MATERIALIZED VIEW covid.segment_aadt;

CREATE MATERIALIZED VIEW covid.segment_aadt
TABLESPACE pg_default
AS
 WITH conflated AS (
         SELECT DISTINCT segment_links_v5_1.segment_id,
            segment_links_v5_1.link_dir,
            centreline.pp_centreline_id AS geo_id,
            centreline.dir AS geo_dir
           FROM here_matched_180430 here
             JOIN ( SELECT gis.direction_from_line(cen.geom) AS dir,
                    cen."shstReferenceId",
                    cen.pp_centreline_id
                   FROM gis_shared_streets.centreline_directional_matched_180430 cen
                     JOIN gis.centreline_directional ON centreline_directional.centreline_id = cen.pp_centreline_id
                  WHERE centreline_directional.feature_code_desc = ANY (ARRAY['Expressway'::text, 'Expressway Ramp'::text, 'Major Arterial'::text, 'Major Arterial Ramp'::text, 'Minor Arterial'::text])) centreline USING ("shstReferenceId")
             JOIN congestion.segment_links_v5 segment_links_v5_1 ON segment_links_v5_1.link_dir = here.pp_link_dir::text
        )
 SELECT segment_links_v5.segment_id,
    avg(link_aadt.aadt) AS seg_aadt
   FROM ( SELECT DISTINCT conflated.link_dir,
            vol.aadt
           FROM conflated
             JOIN covid.teps_aadt_2018 vol ON conflated.geo_id = vol.geo_id AND vol.dir_bin =
                CASE
                    WHEN conflated.geo_dir::text = ANY (ARRAY['Northbound'::character varying::text, 'Eastbound'::character varying::text]) THEN 1
                    ELSE '-1'::integer
                END) link_aadt
     JOIN congestion.segment_links_v5 USING (link_dir)
  GROUP BY segment_links_v5.segment_id
WITH DATA;

ALTER TABLE covid.segment_aadt
    OWNER TO natalie;

COMMENT ON MATERIALIZED VIEW covid.segment_aadt
    IS 'Segmented based aadt conflated with sharedstreets';

GRANT SELECT ON TABLE covid.segment_aadt TO bdit_humans;
GRANT ALL ON TABLE covid.segment_aadt TO natalie;


-- View: covid.segment_aadt_within

-- DROP MATERIALIZED VIEW covid.segment_aadt_within;

CREATE MATERIALIZED VIEW covid.segment_aadt_within
TABLESPACE pg_default
AS
 WITH missing AS (
         SELECT seg_1.segment_id,
            seg_1.link_dir,
            routing_streets_18_3.geom
           FROM congestion.segment_links_v5 seg_1
             JOIN here.routing_streets_18_3 USING (link_dir)
             LEFT JOIN covid.segment_aadt conflated USING (segment_id)
          WHERE conflated.segment_id IS NULL
        ), buff AS (
         SELECT vol.geo_id,
            st_transform(st_buffer(st_transform(cen.geom, 2952), 10::double precision, 'endcap=flat join=round'::text), 4326) AS buff_geom,
            cen.geom,
            vol.dir_bin,
            vol.aadt
           FROM covid.teps_aadt_2018 vol
             JOIN gis.centreline cen USING (geo_id)
          WHERE cen.fcode_desc::text = ANY (ARRAY['Expressway'::text, 'Expressway Ramp'::text, 'Major Arterial'::text, 'Major Arterial Ramp'::text, 'Minor Arterial'::text])
        )
 SELECT seg.segment_id,
    seg.link_dir,
    buff.aadt,
    seg.geom
   FROM missing seg
     JOIN buff ON st_within(seg.geom, buff.buff_geom) AND
        CASE
            WHEN gis.direction_from_line(seg.geom)::text = ANY (ARRAY['Northbound'::text, 'Eastbound'::text]) THEN 1
            ELSE '-1'::integer
        END = buff.dir_bin
WITH DATA;

ALTER TABLE covid.segment_aadt_within
    OWNER TO natalie;

COMMENT ON MATERIALIZED VIEW covid.segment_aadt_within
    IS 'Segment based aadt conflated with buffers';

GRANT SELECT ON TABLE covid.segment_aadt_within TO bdit_humans;
GRANT ALL ON TABLE covid.segment_aadt_within TO natalie;


-- View: covid.segment_aadt_final

-- DROP MATERIALIZED VIEW covid.segment_aadt_final;

CREATE MATERIALIZED VIEW covid.segment_aadt_final
TABLESPACE pg_default
AS
 WITH missing AS (
         SELECT segments_v5_1.segment_id,
            st_transform(st_buffer(st_transform(segments_v5_1.geom, 2952), 15::double precision), 4326) AS geom
           FROM congestion.segments_v5 segments_v5_1
             LEFT JOIN ( SELECT segment_aadt_within_v2.segment_id,
                    avg(segment_aadt_within_v2.aadt) AS aadt
                   FROM covid.segment_aadt_within segment_aadt_within_v2
                  GROUP BY segment_aadt_within_v2.segment_id
                UNION
                 SELECT segment_aadt.segment_id,
                    segment_aadt.seg_aadt
                   FROM covid.segment_aadt) matched USING (segment_id)
          WHERE matched.* IS NULL
        ), new_seg AS (
         SELECT rank() OVER (PARTITION BY missing.segment_id ORDER BY (st_length(st_transform(st_intersection(missing.geom, matched.geom), 2952))) DESC) AS rank,
            missing.segment_id,
            matched.segment_id AS neigh_id,
            matched.aadt,
            missing.geom,
            st_length(st_transform(st_intersection(missing.geom, matched.geom), 2952)) AS length
           FROM missing
             LEFT JOIN ( SELECT a_1.segment_id,
                    segments_v5_1.geom,
                    a_1.aadt,
                    gis.direction_from_line(segments_v5_1.geom) AS dir
                   FROM ( SELECT segment_aadt_within_v2.segment_id,
                            avg(segment_aadt_within_v2.aadt) AS aadt
                           FROM covid.segment_aadt_within segment_aadt_within_v2
                          GROUP BY segment_aadt_within_v2.segment_id
                        UNION
                         SELECT segment_aadt.segment_id,
                            segment_aadt.seg_aadt
                           FROM covid.segment_aadt) a_1
                     JOIN congestion.segments_v5 segments_v5_1 USING (segment_id)) matched ON st_intersects(missing.geom, matched.geom) AND matched.dir::text = gis.direction_from_line(matched.geom)::text
        ), still_missing AS (
         SELECT rank() OVER (PARTITION BY missing.segment_id ORDER BY (st_length(st_transform(st_intersection(missing.geom, matched.geom), 2952))) DESC) AS rank,
            missing.segment_id,
            matched.segment_id AS neigh_id,
            matched.aadt,
            missing.geom,
            st_length(st_transform(st_intersection(missing.geom, matched.geom), 2952)) AS length
           FROM new_seg missing
             LEFT JOIN ( SELECT a_1.segment_id,
                    segments_v5_1.geom,
                    a_1.aadt,
                    gis.direction_from_line(segments_v5_1.geom) AS dir
                   FROM ( SELECT segment_aadt_within_v2.segment_id,
                            avg(segment_aadt_within_v2.aadt) AS aadt
                           FROM covid.segment_aadt_within segment_aadt_within_v2
                          GROUP BY segment_aadt_within_v2.segment_id
                        UNION
                         SELECT segment_aadt.segment_id,
                            segment_aadt.seg_aadt
                           FROM covid.segment_aadt
                        UNION
                         SELECT new_seg.segment_id,
                            new_seg.aadt
                           FROM new_seg
                          WHERE new_seg.rank = 1 AND new_seg.neigh_id IS NOT NULL) a_1
                     JOIN congestion.segments_v5 segments_v5_1 USING (segment_id)) matched ON st_intersects(missing.geom, matched.geom) AND matched.dir::text = gis.direction_from_line(matched.geom)::text
          WHERE missing.neigh_id IS NULL
        ), stillstill_missing AS (
         SELECT rank() OVER (PARTITION BY missing.segment_id ORDER BY (st_length(st_transform(st_intersection(missing.geom, matched.geom), 2952))) DESC) AS rank,
            missing.segment_id,
            matched.segment_id AS neigh_id,
            matched.aadt,
            missing.geom,
            st_length(st_transform(st_intersection(missing.geom, matched.geom), 2952)) AS length
           FROM still_missing missing
             LEFT JOIN ( SELECT a_1.segment_id,
                    segments_v5_1.geom,
                    a_1.aadt,
                    gis.direction_from_line(segments_v5_1.geom) AS dir
                   FROM ( SELECT segment_aadt_within_v2.segment_id,
                            avg(segment_aadt_within_v2.aadt) AS aadt
                           FROM covid.segment_aadt_within segment_aadt_within_v2
                          GROUP BY segment_aadt_within_v2.segment_id
                        UNION
                         SELECT segment_aadt.segment_id,
                            segment_aadt.seg_aadt
                           FROM covid.segment_aadt
                        UNION
                         SELECT new_seg.segment_id,
                            new_seg.aadt
                           FROM new_seg
                          WHERE new_seg.rank = 1 AND new_seg.neigh_id IS NOT NULL
                        UNION
                         SELECT still_missing.segment_id,
                            still_missing.aadt
                           FROM still_missing
                          WHERE still_missing.rank = 1 AND still_missing.neigh_id IS NOT NULL) a_1
                     JOIN congestion.segments_v5 segments_v5_1 USING (segment_id)) matched ON st_intersects(missing.geom, matched.geom) AND matched.dir::text = gis.direction_from_line(matched.geom)::text
          WHERE missing.neigh_id IS NULL
        ), stillstillstill_missing AS (
         SELECT rank() OVER (PARTITION BY missing.segment_id ORDER BY (st_length(st_transform(st_intersection(missing.geom, matched.geom), 2952))) DESC) AS rank,
            missing.segment_id,
            matched.segment_id AS neigh_id,
            matched.aadt,
            missing.geom,
            st_length(st_transform(st_intersection(missing.geom, matched.geom), 2952)) AS length
           FROM stillstill_missing missing
             LEFT JOIN ( SELECT a_1.segment_id,
                    segments_v5_1.geom,
                    a_1.aadt,
                    gis.direction_from_line(segments_v5_1.geom) AS dir
                   FROM ( SELECT segment_aadt_within_v2.segment_id,
                            avg(segment_aadt_within_v2.aadt) AS aadt
                           FROM covid.segment_aadt_within segment_aadt_within_v2
                          GROUP BY segment_aadt_within_v2.segment_id
                        UNION
                         SELECT segment_aadt.segment_id,
                            segment_aadt.seg_aadt
                           FROM covid.segment_aadt
                        UNION
                         SELECT new_seg.segment_id,
                            new_seg.aadt
                           FROM new_seg
                          WHERE new_seg.rank = 1 AND new_seg.neigh_id IS NOT NULL
                        UNION
                         SELECT still_missing.segment_id,
                            still_missing.aadt
                           FROM still_missing
                          WHERE still_missing.rank = 1 AND still_missing.neigh_id IS NOT NULL
                        UNION
                         SELECT stillstill_missing.segment_id,
                            stillstill_missing.aadt
                           FROM stillstill_missing
                          WHERE stillstill_missing.rank = 1 AND stillstill_missing.neigh_id IS NOT NULL) a_1
                     JOIN congestion.segments_v5 segments_v5_1 USING (segment_id)) matched ON st_intersects(missing.geom, matched.geom) AND matched.dir::text = gis.direction_from_line(matched.geom)::text
          WHERE missing.neigh_id IS NULL
        ), final_prep AS (
         SELECT a.segment_id,
            segments_v5.geom,
            a.aadt,
            a.length
           FROM ( SELECT segment_aadt_within_v2.segment_id,
                    avg(segment_aadt_within_v2.aadt) AS aadt,
                    100000 AS length
                   FROM covid.segment_aadt_within segment_aadt_within_v2
                  GROUP BY segment_aadt_within_v2.segment_id
                UNION
                 SELECT segment_aadt.segment_id,
                    segment_aadt.seg_aadt,
                    100000 AS length
                   FROM covid.segment_aadt
                UNION
                 SELECT new_seg.segment_id,
                    new_seg.aadt,
                    new_seg.length
                   FROM new_seg
                  WHERE new_seg.rank = 1 AND new_seg.neigh_id IS NOT NULL
                UNION
                 SELECT still_missing.segment_id,
                    still_missing.aadt,
                    still_missing.length
                   FROM still_missing
                  WHERE still_missing.rank = 1 AND still_missing.neigh_id IS NOT NULL
                UNION
                 SELECT stillstill_missing.segment_id,
                    stillstill_missing.aadt,
                    stillstill_missing.length
                   FROM stillstill_missing
                  WHERE stillstill_missing.rank = 1 AND stillstill_missing.neigh_id IS NOT NULL
                UNION
                 SELECT stillstillstill_missing.segment_id,
                    stillstillstill_missing.aadt,
                    stillstillstill_missing.length
                   FROM stillstillstill_missing
                  WHERE stillstillstill_missing.rank = 1 AND stillstillstill_missing.neigh_id IS NOT NULL) a
             JOIN congestion.segments_v5 USING (segment_id)
        )
 SELECT DISTINCT ON (final_prep.segment_id) final_prep.segment_id,
    final_prep.geom,
    final_prep.aadt
   FROM final_prep
  ORDER BY final_prep.segment_id, final_prep.length DESC
WITH DATA;

ALTER TABLE covid.segment_aadt_final
    OWNER TO natalie;

GRANT SELECT ON TABLE covid.segment_aadt_final TO congestion_admins;
GRANT SELECT ON TABLE covid.segment_aadt_final TO bdit_humans;
GRANT ALL ON TABLE covid.segment_aadt_final TO natalie;