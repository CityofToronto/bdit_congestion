-- View: congestion.mergedshort_result1_nextbest

-- DROP MATERIALIZED VIEW congestion.mergedshort_result1_nextbest;

CREATE MATERIALIZED VIEW congestion.mergedshort_result1_nextbest
TABLESPACE pg_default
AS
 WITH temp AS (
         SELECT mergedshort_result1.segment_id,
            mergedshort_result1.start_vid,
            mergedshort_result1.end_vid,
            mergedshort_result1.link_set,
            mergedshort_result1.geom,
            st_length(st_transform(mergedshort_result1.geom, 2952)) AS length,
            degrees(st_azimuth(st_startpoint(st_line_substring(mergedshort_result1.geom, 0::double precision, 2::double precision / st_length(st_transform(mergedshort_result1.geom, 2952)))), st_endpoint(st_line_substring(mergedshort_result1.geom, 0::double precision, 2::double precision / st_length(st_transform(mergedshort_result1.geom, 2952)))))) AS start_subsec,
            degrees(st_azimuth(st_startpoint(st_line_substring(mergedshort_result1.geom, (st_length(st_transform(mergedshort_result1.geom, 2952)) - 2::double precision) / st_length(st_transform(mergedshort_result1.geom, 2952)), 1::double precision)), st_endpoint(st_line_substring(mergedshort_result1.geom, (st_length(st_transform(mergedshort_result1.geom, 2952)) - 2::double precision) / st_length(st_transform(mergedshort_result1.geom, 2952)), 1::double precision)))) AS end_subsec
           FROM congestion.mergedshort_result1
        ), cal AS (
         SELECT pot.segment_id AS pot_seg,
            pot.start_vid AS pot_start_vid,
            pot.end_vid AS pot_end_vid,
            pot.link_set AS pot_link_set,
            pot.geom AS pot_geom,
            st_length(st_transform(pot.geom, 2952)) AS pot_length,
                CASE
                    WHEN pot.start_vid = temp.end_vid THEN degrees(st_azimuth(st_startpoint(st_line_substring(pot.geom, 0::double precision, 2::double precision / st_length(st_transform(pot.geom, 2952)))), st_endpoint(st_line_substring(pot.geom, 0::double precision, 2::double precision / st_length(st_transform(pot.geom, 2952))))))
                    ELSE degrees(st_azimuth(st_startpoint(st_line_substring(pot.geom, (st_length(st_transform(pot.geom, 2952)) - 2::double precision) / st_length(st_transform(pot.geom, 2952)), 1::double precision)), st_endpoint(st_line_substring(pot.geom, (st_length(st_transform(pot.geom, 2952)) - 2::double precision) / st_length(st_transform(pot.geom, 2952)), 1::double precision))))
                END AS pot_az,
            temp.segment_id,
            temp.start_vid,
            temp.end_vid,
            temp.link_set,
            temp.geom,
            temp.length,
            temp.start_subsec,
            temp.end_subsec
           FROM congestion.mergedshort_result1 pot,
            temp
          WHERE (pot.start_vid = temp.end_vid OR pot.end_vid = temp.start_vid) AND NOT (pot.start_vid = temp.end_vid AND pot.end_vid = temp.start_vid)
        ), next_best AS (
         SELECT cal.pot_seg,
            cal.pot_start_vid,
            cal.pot_end_vid,
            cal.pot_link_set,
            cal.pot_geom,
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END AS ang_diff,
            cal.pot_length,
            cal.length,
            cal.segment_id,
            cal.start_vid,
            cal.end_vid,
            cal.link_set,
            cal.geom
           FROM cal
          ORDER BY (
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END)
        )
 SELECT row_number() OVER (PARTITION BY a.segment_id, a.length, a.start_vid, a.end_vid, a.link_set, a.geom ORDER BY b.ang_diff) AS rank,
    a.segment_id,
    a.start_vid,
    a.end_vid,
    a.link_set,
    a.length,
    a.geom,
    b.pot_seg,
    b.pot_start_vid,
    b.pot_end_vid,
    b.pot_link_set,
    b.pot_length,
    b.pot_geom,
    b.ang_diff
   FROM temp a
     LEFT JOIN LATERAL ( SELECT next_best.pot_seg,
            next_best.pot_length,
            next_best.pot_start_vid,
            next_best.pot_end_vid,
            next_best.pot_link_set,
            next_best.pot_geom,
            next_best.ang_diff
           FROM next_best
          WHERE a.start_vid = next_best.start_vid AND a.end_vid = next_best.end_vid
         LIMIT 2) b ON true
WITH DATA;

ALTER TABLE congestion.mergedshort_result1_nextbest
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.mergedshort_result1_nextbest TO bdit_humans;
GRANT ALL ON TABLE congestion.mergedshort_result1_nextbest TO natalie;


-- View: congestion.mergedshort_result1_ordered

-- DROP MATERIALIZED VIEW congestion.mergedshort_result1_ordered;

CREATE MATERIALIZED VIEW congestion.mergedshort_result1_ordered
TABLESPACE pg_default
AS
 WITH rank AS (
         SELECT a.start_vid,
            a.end_vid,
            sum(linkdir_obs.total_count) AS sum
           FROM ( SELECT DISTINCT route_nextbest_1.start_vid,
                    route_nextbest_1.end_vid,
                    unnest(route_nextbest_1.link_set) AS unnest
                   FROM congestion.mergedshort_result1_nextbest route_nextbest_1) a
             JOIN congestion.routing_grid ON a.unnest = routing_grid.link_dir
             JOIN congestion.linkdir_obs USING (link_dir)
          GROUP BY a.start_vid, a.end_vid
        ), prep AS (
         SELECT
                CASE
                    WHEN route_int2int_nextbest.length < 50::double precision THEN 1
                    WHEN route_int2int_nextbest.length >= 50::double precision AND route_int2int_nextbest.length < 100::double precision THEN 2
                    WHEN route_int2int_nextbest.length >= 100::double precision AND route_int2int_nextbest.length <= 200::double precision THEN 3
                    ELSE 4
                END AS length_rank,
            route_int2int_nextbest.start_vid,
            route_int2int_nextbest.end_vid,
            route_int2int_nextbest.rank,
            route_int2int_nextbest.segment_id,
            route_int2int_nextbest.link_set,
            route_int2int_nextbest.length,
            route_int2int_nextbest.geom,
            route_int2int_nextbest.pot_seg,
            route_int2int_nextbest.pot_start_vid,
            route_int2int_nextbest.pot_end_vid,
            route_int2int_nextbest.pot_link_set,
            route_int2int_nextbest.pot_length,
            route_int2int_nextbest.pot_geom,
            route_int2int_nextbest.ang_diff,
            rank.sum
           FROM congestion.mergedshort_result1_nextbest route_int2int_nextbest
             JOIN rank USING (start_vid, end_vid)
        )
 SELECT prep.start_vid,
    prep.end_vid,
    prep.rank,
    prep.segment_id,
    prep.link_set,
    prep.length,
    prep.geom,
    prep.pot_seg,
    prep.pot_start_vid,
    prep.pot_end_vid,
    prep.pot_link_set,
    prep.pot_length,
    prep.pot_geom,
    prep.ang_diff,
    prep.sum
   FROM prep
  ORDER BY prep.length_rank, prep.sum DESC, prep.rank
WITH DATA;

ALTER TABLE congestion.mergedshort_result1_ordered
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.mergedshort_result1_ordered TO bdit_humans;
GRANT ALL ON TABLE congestion.mergedshort_result1_ordered TO natalie;

-- View: congestion.mergedshort_result2_nextbest

-- DROP MATERIALIZED VIEW congestion.mergedshort_result2_nextbest;

CREATE MATERIALIZED VIEW congestion.mergedshort_result2_nextbest
TABLESPACE pg_default
AS
 WITH temp AS (
         SELECT mergedshort_result1.segment_id,
            mergedshort_result1.start_vid,
            mergedshort_result1.end_vid,
            mergedshort_result1.link_set,
            mergedshort_result1.geom,
            st_length(st_transform(mergedshort_result1.geom, 2952)) AS length,
            degrees(st_azimuth(st_startpoint(st_line_substring(mergedshort_result1.geom, 0::double precision, 2::double precision / st_length(st_transform(mergedshort_result1.geom, 2952)))), st_endpoint(st_line_substring(mergedshort_result1.geom, 0::double precision, 2::double precision / st_length(st_transform(mergedshort_result1.geom, 2952)))))) AS start_subsec,
            degrees(st_azimuth(st_startpoint(st_line_substring(mergedshort_result1.geom, (st_length(st_transform(mergedshort_result1.geom, 2952)) - 2::double precision) / st_length(st_transform(mergedshort_result1.geom, 2952)), 1::double precision)), st_endpoint(st_line_substring(mergedshort_result1.geom, (st_length(st_transform(mergedshort_result1.geom, 2952)) - 2::double precision) / st_length(st_transform(mergedshort_result1.geom, 2952)), 1::double precision)))) AS end_subsec
           FROM congestion.mergedshort_result2 mergedshort_result1
        ), cal AS (
         SELECT pot.segment_id AS pot_seg,
            pot.start_vid AS pot_start_vid,
            pot.end_vid AS pot_end_vid,
            pot.link_set AS pot_link_set,
            pot.geom AS pot_geom,
            st_length(st_transform(pot.geom, 2952)) AS pot_length,
                CASE
                    WHEN pot.start_vid = temp.end_vid THEN degrees(st_azimuth(st_startpoint(st_line_substring(pot.geom, 0::double precision, 2::double precision / st_length(st_transform(pot.geom, 2952)))), st_endpoint(st_line_substring(pot.geom, 0::double precision, 2::double precision / st_length(st_transform(pot.geom, 2952))))))
                    ELSE degrees(st_azimuth(st_startpoint(st_line_substring(pot.geom, (st_length(st_transform(pot.geom, 2952)) - 2::double precision) / st_length(st_transform(pot.geom, 2952)), 1::double precision)), st_endpoint(st_line_substring(pot.geom, (st_length(st_transform(pot.geom, 2952)) - 2::double precision) / st_length(st_transform(pot.geom, 2952)), 1::double precision))))
                END AS pot_az,
            temp.segment_id,
            temp.start_vid,
            temp.end_vid,
            temp.link_set,
            temp.geom,
            temp.length,
            temp.start_subsec,
            temp.end_subsec
           FROM congestion.mergedshort_result2 pot,
            temp
          WHERE (pot.start_vid = temp.end_vid OR pot.end_vid = temp.start_vid) AND NOT (pot.start_vid = temp.end_vid AND pot.end_vid = temp.start_vid)
        ), next_best AS (
         SELECT cal.pot_seg,
            cal.pot_start_vid,
            cal.pot_end_vid,
            cal.pot_link_set,
            cal.pot_geom,
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END AS ang_diff,
            cal.pot_length,
            cal.length,
            cal.segment_id,
            cal.start_vid,
            cal.end_vid,
            cal.link_set,
            cal.geom
           FROM cal
          ORDER BY (
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END)
        )
 SELECT row_number() OVER (PARTITION BY a.segment_id, a.length, a.start_vid, a.end_vid, a.link_set, a.geom ORDER BY b.ang_diff) AS rank,
    a.segment_id,
    a.start_vid,
    a.end_vid,
    a.link_set,
    a.length,
    a.geom,
    b.pot_seg,
    b.pot_start_vid,
    b.pot_end_vid,
    b.pot_link_set,
    b.pot_length,
    b.pot_geom,
    b.ang_diff
   FROM temp a
     LEFT JOIN LATERAL ( SELECT next_best.pot_seg,
            next_best.pot_length,
            next_best.pot_start_vid,
            next_best.pot_end_vid,
            next_best.pot_link_set,
            next_best.pot_geom,
            next_best.ang_diff
           FROM next_best
          WHERE a.start_vid = next_best.start_vid AND a.end_vid = next_best.end_vid
         LIMIT 2) b ON true
WITH DATA;

ALTER TABLE congestion.mergedshort_result2_nextbest
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.mergedshort_result2_nextbest TO bdit_humans;
GRANT ALL ON TABLE congestion.mergedshort_result2_nextbest TO natalie;

-- View: congestion.mergedshort_result2_ordered

-- DROP MATERIALIZED VIEW congestion.mergedshort_result2_ordered;

CREATE MATERIALIZED VIEW congestion.mergedshort_result2_ordered
TABLESPACE pg_default
AS
 WITH rank AS (
         SELECT a.start_vid,
            a.end_vid,
            sum(linkdir_obs.total_count) AS sum
           FROM ( SELECT DISTINCT route_nextbest_1.start_vid,
                    route_nextbest_1.end_vid,
                    unnest(route_nextbest_1.link_set) AS unnest
                   FROM congestion.mergedshort_result2_nextbest route_nextbest_1) a
             JOIN congestion.routing_grid ON a.unnest = routing_grid.link_dir
             JOIN congestion.linkdir_obs USING (link_dir)
          GROUP BY a.start_vid, a.end_vid
        ), prep AS (
         SELECT
                CASE
                    WHEN route_int2int_nextbest.length < 50::double precision THEN 1
                    WHEN route_int2int_nextbest.length >= 50::double precision AND route_int2int_nextbest.length < 100::double precision THEN 2
                    WHEN route_int2int_nextbest.length >= 100::double precision AND route_int2int_nextbest.length <= 200::double precision THEN 3
                    ELSE 4
                END AS length_rank,
            route_int2int_nextbest.start_vid,
            route_int2int_nextbest.end_vid,
            route_int2int_nextbest.rank,
            route_int2int_nextbest.segment_id,
            route_int2int_nextbest.link_set,
            route_int2int_nextbest.length,
            route_int2int_nextbest.geom,
            route_int2int_nextbest.pot_seg,
            route_int2int_nextbest.pot_start_vid,
            route_int2int_nextbest.pot_end_vid,
            route_int2int_nextbest.pot_link_set,
            route_int2int_nextbest.pot_length,
            route_int2int_nextbest.pot_geom,
            route_int2int_nextbest.ang_diff,
            rank.sum
           FROM congestion.mergedshort_result2_nextbest route_int2int_nextbest
             JOIN rank USING (start_vid, end_vid)
        )
 SELECT prep.start_vid,
    prep.end_vid,
    prep.rank,
    prep.segment_id,
    prep.link_set,
    prep.length,
    prep.geom,
    prep.pot_seg,
    prep.pot_start_vid,
    prep.pot_end_vid,
    prep.pot_link_set,
    prep.pot_length,
    prep.pot_geom,
    prep.ang_diff,
    prep.sum
   FROM prep
  ORDER BY prep.length_rank, prep.sum DESC, prep.rank
WITH DATA;

ALTER TABLE congestion.mergedshort_result2_ordered
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.mergedshort_result2_ordered TO bdit_humans;
GRANT ALL ON TABLE congestion.mergedshort_result2_ordered TO natalie;

-- View: congestion.mergeshort_result1_ordered_v2

-- DROP MATERIALIZED VIEW congestion.mergeshort_result1_ordered_v2;

CREATE MATERIALIZED VIEW congestion.mergeshort_result1_ordered_v2
TABLESPACE pg_default
AS
 WITH prep_merged_table AS (
         SELECT a.segment_id,
            a.start_vid,
            a.end_vid,
            a.link_set,
            unnest(a.link_set) AS links,
            a.length_set,
            a.length
           FROM ( SELECT row_number() OVER () AS segment_id,
                    mergeshort_result1_v2.start_vid,
                    mergeshort_result1_v2.end_vid,
                    mergeshort_result1_v2.link_set,
                    mergeshort_result1_v2.length_set,
                    mergeshort_result1_v2.length
                   FROM congestion.mergeshort_result1_v2) a
        ), merged_table AS (
         SELECT prep_merged_table.segment_id,
            prep_merged_table.start_vid,
            prep_merged_table.end_vid,
            prep_merged_table.link_set,
            prep_merged_table.length_set,
            prep_merged_table.length,
            st_linemerge(st_union(routing_grid.geom)) AS geom,
            sum(linkdir_obs.total_count) AS sum_obs
           FROM prep_merged_table
             JOIN congestion.routing_grid ON prep_merged_table.links = routing_grid.id
             JOIN congestion.linkdir_obs USING (link_dir)
          GROUP BY prep_merged_table.segment_id, prep_merged_table.start_vid, prep_merged_table.end_vid, prep_merged_table.link_set, prep_merged_table.length_set, prep_merged_table.length
        ), temp AS (
         SELECT route_int2int.segment_id,
            route_int2int.start_vid,
            route_int2int.end_vid,
            route_int2int.link_set,
            route_int2int.length_set,
            route_int2int.geom,
            route_int2int.length,
            route_int2int.sum_obs,
            degrees(st_azimuth(st_startpoint(st_linesubstring(route_int2int.geom, 0::double precision, 2::double precision / route_int2int.length::double precision)), st_endpoint(st_linesubstring(route_int2int.geom, 0::double precision, 2::double precision / route_int2int.length::double precision)))) AS start_subsec,
            degrees(st_azimuth(st_startpoint(st_linesubstring(route_int2int.geom, (route_int2int.length::double precision - 2::double precision) / route_int2int.length::double precision, 1::double precision)), st_endpoint(st_linesubstring(route_int2int.geom, (route_int2int.length::double precision - 2::double precision) / route_int2int.length::double precision, 1::double precision)))) AS end_subsec
           FROM merged_table route_int2int
        ), cal AS (
         SELECT pot.segment_id AS pot_seg,
            pot.start_vid AS pot_start_vid,
            pot.end_vid AS pot_end_vid,
            pot.link_set AS pot_link_set,
            pot.length_set AS pot_length_set,
            pot.geom AS pot_geom,
            pot.length AS pot_length,
                CASE
                    WHEN pot.start_vid = temp.end_vid THEN degrees(st_azimuth(st_startpoint(st_linesubstring(pot.geom, 0::double precision, 2::double precision / pot.length::double precision)), st_endpoint(st_linesubstring(pot.geom, 0::double precision, 2::double precision / pot.length::double precision))))
                    ELSE degrees(st_azimuth(st_startpoint(st_linesubstring(pot.geom, (pot.length::double precision - 2::double precision) / pot.length::double precision, 1::double precision)), st_endpoint(st_linesubstring(pot.geom, (pot.length::double precision - 2::double precision) / pot.length::double precision, 1::double precision))))
                END AS pot_az,
            temp.segment_id,
            temp.start_vid,
            temp.end_vid,
            temp.link_set,
            temp.length_set,
            temp.geom,
            temp.length,
            temp.sum_obs,
            temp.start_subsec,
            temp.end_subsec
           FROM merged_table pot,
            temp
          WHERE (pot.start_vid = temp.end_vid OR pot.end_vid = temp.start_vid) AND NOT (pot.start_vid = temp.end_vid AND pot.end_vid = temp.start_vid)
        ), next_best AS (
         SELECT cal.pot_seg,
            cal.pot_start_vid,
            cal.pot_end_vid,
            cal.pot_link_set,
            cal.pot_length_set,
            cal.pot_geom,
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END AS ang_diff,
            cal.pot_length,
            cal.length,
            cal.segment_id,
            cal.start_vid,
            cal.end_vid,
            cal.link_set,
            cal.length_set,
            cal.sum_obs,
            cal.geom
           FROM cal
          ORDER BY (
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END), cal.length
        ), selections AS (
         SELECT row_number() OVER (PARTITION BY a.segment_id, a.length, a.start_vid, a.end_vid, a.link_set, a.geom ORDER BY b.ang_diff) AS rank,
            a.segment_id,
            a.start_vid,
            a.end_vid,
            a.link_set,
            a.length_set,
            a.length,
            a.sum_obs,
            a.geom,
            b.pot_seg,
            b.pot_start_vid,
            b.pot_end_vid,
            b.pot_link_set,
            b.pot_length_set,
            b.pot_length,
            b.pot_geom,
            b.ang_diff
           FROM temp a
             LEFT JOIN LATERAL ( SELECT next_best.pot_seg,
                    next_best.pot_length,
                    next_best.pot_start_vid,
                    next_best.pot_end_vid,
                    next_best.pot_link_set,
                    next_best.pot_length_set,
                    next_best.pot_geom,
                    next_best.ang_diff
                   FROM next_best
                  WHERE a.start_vid = next_best.start_vid AND a.end_vid = next_best.end_vid
                 LIMIT 2) b ON true
        ), prep AS (
         SELECT
                CASE
                    WHEN selections.length::double precision < 50::double precision THEN 1
                    WHEN selections.length::double precision >= 50::double precision AND selections.length::double precision < 100::double precision THEN 2
                    WHEN selections.length::double precision >= 100::double precision AND selections.length::double precision <= 200::double precision THEN 3
                    ELSE 4
                END AS length_rank,
            selections.start_vid,
            selections.end_vid,
            selections.rank,
            selections.segment_id,
            selections.link_set,
            selections.length_set,
            selections.length,
            selections.geom,
            selections.pot_seg,
            selections.pot_start_vid,
            selections.pot_end_vid,
            selections.pot_link_set,
            selections.pot_length_set,
            selections.pot_length,
            selections.pot_geom,
            selections.ang_diff,
            selections.sum_obs
           FROM selections
        )
 SELECT prep.start_vid,
    prep.end_vid,
    prep.rank,
    prep.segment_id,
    prep.link_set,
    prep.length_set,
    prep.length,
    prep.geom,
    prep.pot_seg,
    prep.pot_start_vid,
    prep.pot_end_vid,
    prep.pot_link_set,
    prep.pot_length_set,
    prep.pot_length,
    prep.pot_geom,
    prep.ang_diff,
    prep.sum_obs
   FROM prep
  ORDER BY prep.length_rank, prep.sum_obs DESC, prep.rank
WITH DATA;

ALTER TABLE congestion.mergeshort_result1_ordered_v2
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.mergeshort_result1_ordered_v2 TO bdit_humans;
GRANT ALL ON TABLE congestion.mergeshort_result1_ordered_v2 TO natalie;

-- View: congestion.mergeshort_result2_ordered_v2

-- DROP MATERIALIZED VIEW congestion.mergeshort_result2_ordered_v2;

CREATE MATERIALIZED VIEW congestion.mergeshort_result2_ordered_v2
TABLESPACE pg_default
AS
 WITH prep_merged_table AS (
         SELECT a.segment_id,
            a.start_vid,
            a.end_vid,
            a.link_set,
            unnest(a.link_set) AS links,
            a.length_set,
            a.length
           FROM ( SELECT row_number() OVER () AS segment_id,
                    mergeshort_result2_v2.start_vid,
                    mergeshort_result2_v2.end_vid,
                    mergeshort_result2_v2.link_set,
                    mergeshort_result2_v2.length_set,
                    mergeshort_result2_v2.length
                   FROM congestion.mergeshort_result2_v2) a
        ), merged_table AS (
         SELECT prep_merged_table.segment_id,
            prep_merged_table.start_vid,
            prep_merged_table.end_vid,
            prep_merged_table.link_set,
            prep_merged_table.length_set,
            prep_merged_table.length,
            st_linemerge(st_union(routing_grid.geom)) AS geom,
            sum(linkdir_obs.total_count) AS sum_obs
           FROM prep_merged_table
             JOIN congestion.routing_grid ON prep_merged_table.links = routing_grid.id
             JOIN congestion.linkdir_obs USING (link_dir)
          GROUP BY prep_merged_table.segment_id, prep_merged_table.start_vid, prep_merged_table.end_vid, prep_merged_table.link_set, prep_merged_table.length_set, prep_merged_table.length
        ), temp AS (
         SELECT route_int2int.segment_id,
            route_int2int.start_vid,
            route_int2int.end_vid,
            route_int2int.link_set,
            route_int2int.length_set,
            route_int2int.geom,
            route_int2int.length,
            route_int2int.sum_obs,
            degrees(st_azimuth(st_startpoint(st_linesubstring(route_int2int.geom, 0::double precision, 2::double precision / route_int2int.length::double precision)), st_endpoint(st_linesubstring(route_int2int.geom, 0::double precision, 2::double precision / route_int2int.length::double precision)))) AS start_subsec,
            degrees(st_azimuth(st_startpoint(st_linesubstring(route_int2int.geom, (route_int2int.length::double precision - 2::double precision) / route_int2int.length::double precision, 1::double precision)), st_endpoint(st_linesubstring(route_int2int.geom, (route_int2int.length::double precision - 2::double precision) / route_int2int.length::double precision, 1::double precision)))) AS end_subsec
           FROM merged_table route_int2int
        ), cal AS (
         SELECT pot.segment_id AS pot_seg,
            pot.start_vid AS pot_start_vid,
            pot.end_vid AS pot_end_vid,
            pot.link_set AS pot_link_set,
            pot.length_set AS pot_length_set,
            pot.geom AS pot_geom,
            pot.length AS pot_length,
                CASE
                    WHEN pot.start_vid = temp.end_vid THEN degrees(st_azimuth(st_startpoint(st_linesubstring(pot.geom, 0::double precision, 2::double precision / pot.length::double precision)), st_endpoint(st_linesubstring(pot.geom, 0::double precision, 2::double precision / pot.length::double precision))))
                    ELSE degrees(st_azimuth(st_startpoint(st_linesubstring(pot.geom, (pot.length::double precision - 2::double precision) / pot.length::double precision, 1::double precision)), st_endpoint(st_linesubstring(pot.geom, (pot.length::double precision - 2::double precision) / pot.length::double precision, 1::double precision))))
                END AS pot_az,
            temp.segment_id,
            temp.start_vid,
            temp.end_vid,
            temp.link_set,
            temp.length_set,
            temp.geom,
            temp.length,
            temp.sum_obs,
            temp.start_subsec,
            temp.end_subsec
           FROM merged_table pot,
            temp
          WHERE (pot.start_vid = temp.end_vid OR pot.end_vid = temp.start_vid) AND NOT (pot.start_vid = temp.end_vid AND pot.end_vid = temp.start_vid)
        ), next_best AS (
         SELECT cal.pot_seg,
            cal.pot_start_vid,
            cal.pot_end_vid,
            cal.pot_link_set,
            cal.pot_length_set,
            cal.pot_geom,
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END AS ang_diff,
            cal.pot_length,
            cal.length,
            cal.segment_id,
            cal.start_vid,
            cal.end_vid,
            cal.link_set,
            cal.length_set,
            cal.sum_obs,
            cal.geom
           FROM cal
          ORDER BY (
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END), cal.length
        ), selections AS (
         SELECT row_number() OVER (PARTITION BY a.segment_id, a.length, a.start_vid, a.end_vid, a.link_set, a.geom ORDER BY b.ang_diff) AS rank,
            a.segment_id,
            a.start_vid,
            a.end_vid,
            a.link_set,
            a.length_set,
            a.length,
            a.sum_obs,
            a.geom,
            b.pot_seg,
            b.pot_start_vid,
            b.pot_end_vid,
            b.pot_link_set,
            b.pot_length_set,
            b.pot_length,
            b.pot_geom,
            b.ang_diff
           FROM temp a
             LEFT JOIN LATERAL ( SELECT next_best.pot_seg,
                    next_best.pot_length,
                    next_best.pot_start_vid,
                    next_best.pot_end_vid,
                    next_best.pot_link_set,
                    next_best.pot_length_set,
                    next_best.pot_geom,
                    next_best.ang_diff
                   FROM next_best
                  WHERE a.start_vid = next_best.start_vid AND a.end_vid = next_best.end_vid
                 LIMIT 2) b ON true
        ), prep AS (
         SELECT
                CASE
                    WHEN selections.length::double precision < 50::double precision THEN 1
                    WHEN selections.length::double precision >= 50::double precision AND selections.length::double precision < 100::double precision THEN 2
                    WHEN selections.length::double precision >= 100::double precision AND selections.length::double precision <= 200::double precision THEN 3
                    ELSE 4
                END AS length_rank,
            selections.start_vid,
            selections.end_vid,
            selections.rank,
            selections.segment_id,
            selections.link_set,
            selections.length_set,
            selections.length,
            selections.geom,
            selections.pot_seg,
            selections.pot_start_vid,
            selections.pot_end_vid,
            selections.pot_link_set,
            selections.pot_length_set,
            selections.pot_length,
            selections.pot_geom,
            selections.ang_diff,
            selections.sum_obs
           FROM selections
        )
 SELECT prep.start_vid,
    prep.end_vid,
    prep.rank,
    prep.segment_id,
    prep.link_set,
    prep.length_set,
    prep.length,
    prep.geom,
    prep.pot_seg,
    prep.pot_start_vid,
    prep.pot_end_vid,
    prep.pot_link_set,
    prep.pot_length_set,
    prep.pot_length,
    prep.pot_geom,
    prep.ang_diff,
    prep.sum_obs
   FROM prep
  ORDER BY prep.length_rank, prep.sum_obs DESC, prep.rank
WITH DATA;

ALTER TABLE congestion.mergeshort_result2_ordered_v2
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.mergeshort_result2_ordered_v2 TO bdit_humans;
GRANT ALL ON TABLE congestion.mergeshort_result2_ordered_v2 TO natalie;

-- View: congestion.mergeshort_result3_ordered_v2

-- DROP MATERIALIZED VIEW congestion.mergeshort_result3_ordered_v2;

CREATE MATERIALIZED VIEW congestion.mergeshort_result3_ordered_v2
TABLESPACE pg_default
AS
 WITH prep_merged_table AS (
         SELECT a.segment_id,
            a.start_vid,
            a.end_vid,
            a.link_set,
            unnest(a.link_set) AS links,
            a.length_set,
            a.length
           FROM ( SELECT row_number() OVER () AS segment_id,
                    mergeshort_result3_v2.start_vid,
                    mergeshort_result3_v2.end_vid,
                    mergeshort_result3_v2.link_set,
                    mergeshort_result3_v2.length_set,
                    mergeshort_result3_v2.length
                   FROM congestion.mergeshort_result3_v2) a
        ), merged_table AS (
         SELECT prep_merged_table.segment_id,
            prep_merged_table.start_vid,
            prep_merged_table.end_vid,
            prep_merged_table.link_set,
            prep_merged_table.length_set,
            prep_merged_table.length,
            st_linemerge(st_union(routing_grid.geom)) AS geom,
            sum(linkdir_obs.total_count) AS sum_obs
           FROM prep_merged_table
             JOIN congestion.routing_grid ON prep_merged_table.links = routing_grid.id
             JOIN congestion.linkdir_obs USING (link_dir)
          GROUP BY prep_merged_table.segment_id, prep_merged_table.start_vid, prep_merged_table.end_vid, prep_merged_table.link_set, prep_merged_table.length_set, prep_merged_table.length
        ), temp AS (
         SELECT route_int2int.segment_id,
            route_int2int.start_vid,
            route_int2int.end_vid,
            route_int2int.link_set,
            route_int2int.length_set,
            route_int2int.geom,
            route_int2int.length,
            route_int2int.sum_obs,
            degrees(st_azimuth(st_startpoint(st_linesubstring(route_int2int.geom, 0::double precision, 2::double precision / route_int2int.length::double precision)), st_endpoint(st_linesubstring(route_int2int.geom, 0::double precision, 2::double precision / route_int2int.length::double precision)))) AS start_subsec,
            degrees(st_azimuth(st_startpoint(st_linesubstring(route_int2int.geom, (route_int2int.length::double precision - 2::double precision) / route_int2int.length::double precision, 1::double precision)), st_endpoint(st_linesubstring(route_int2int.geom, (route_int2int.length::double precision - 2::double precision) / route_int2int.length::double precision, 1::double precision)))) AS end_subsec
           FROM merged_table route_int2int
        ), cal AS (
         SELECT pot.segment_id AS pot_seg,
            pot.start_vid AS pot_start_vid,
            pot.end_vid AS pot_end_vid,
            pot.link_set AS pot_link_set,
            pot.length_set AS pot_length_set,
            pot.geom AS pot_geom,
            pot.length AS pot_length,
                CASE
                    WHEN pot.start_vid = temp.end_vid THEN degrees(st_azimuth(st_startpoint(st_linesubstring(pot.geom, 0::double precision, 2::double precision / pot.length::double precision)), st_endpoint(st_linesubstring(pot.geom, 0::double precision, 2::double precision / pot.length::double precision))))
                    ELSE degrees(st_azimuth(st_startpoint(st_linesubstring(pot.geom, (pot.length::double precision - 2::double precision) / pot.length::double precision, 1::double precision)), st_endpoint(st_linesubstring(pot.geom, (pot.length::double precision - 2::double precision) / pot.length::double precision, 1::double precision))))
                END AS pot_az,
            temp.segment_id,
            temp.start_vid,
            temp.end_vid,
            temp.link_set,
            temp.length_set,
            temp.geom,
            temp.length,
            temp.sum_obs,
            temp.start_subsec,
            temp.end_subsec
           FROM merged_table pot,
            temp
          WHERE (pot.start_vid = temp.end_vid OR pot.end_vid = temp.start_vid) AND NOT (pot.start_vid = temp.end_vid AND pot.end_vid = temp.start_vid)
        ), next_best AS (
         SELECT cal.pot_seg,
            cal.pot_start_vid,
            cal.pot_end_vid,
            cal.pot_link_set,
            cal.pot_length_set,
            cal.pot_geom,
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END AS ang_diff,
            cal.pot_length,
            cal.length,
            cal.segment_id,
            cal.start_vid,
            cal.end_vid,
            cal.link_set,
            cal.length_set,
            cal.sum_obs,
            cal.geom
           FROM cal
          ORDER BY (
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END), cal.length
        ), selections AS (
         SELECT row_number() OVER (PARTITION BY a.segment_id, a.length, a.start_vid, a.end_vid, a.link_set, a.geom ORDER BY b.ang_diff) AS rank,
            a.segment_id,
            a.start_vid,
            a.end_vid,
            a.link_set,
            a.length_set,
            a.length,
            a.sum_obs,
            a.geom,
            b.pot_seg,
            b.pot_start_vid,
            b.pot_end_vid,
            b.pot_link_set,
            b.pot_length_set,
            b.pot_length,
            b.pot_geom,
            b.ang_diff
           FROM temp a
             LEFT JOIN LATERAL ( SELECT next_best.pot_seg,
                    next_best.pot_length,
                    next_best.pot_start_vid,
                    next_best.pot_end_vid,
                    next_best.pot_link_set,
                    next_best.pot_length_set,
                    next_best.pot_geom,
                    next_best.ang_diff
                   FROM next_best
                  WHERE a.start_vid = next_best.start_vid AND a.end_vid = next_best.end_vid
                 LIMIT 2) b ON true
        ), prep AS (
         SELECT
                CASE
                    WHEN selections.length::double precision < 50::double precision THEN 1
                    WHEN selections.length::double precision >= 50::double precision AND selections.length::double precision < 100::double precision THEN 2
                    WHEN selections.length::double precision >= 100::double precision AND selections.length::double precision <= 200::double precision THEN 3
                    ELSE 4
                END AS length_rank,
            selections.start_vid,
            selections.end_vid,
            selections.rank,
            selections.segment_id,
            selections.link_set,
            selections.length_set,
            selections.length,
            selections.geom,
            selections.pot_seg,
            selections.pot_start_vid,
            selections.pot_end_vid,
            selections.pot_link_set,
            selections.pot_length_set,
            selections.pot_length,
            selections.pot_geom,
            selections.ang_diff,
            selections.sum_obs
           FROM selections
        )
 SELECT prep.start_vid,
    prep.end_vid,
    prep.rank,
    prep.segment_id,
    prep.link_set,
    prep.length_set,
    prep.length,
    prep.geom,
    prep.pot_seg,
    prep.pot_start_vid,
    prep.pot_end_vid,
    prep.pot_link_set,
    prep.pot_length_set,
    prep.pot_length,
    prep.pot_geom,
    prep.ang_diff,
    prep.sum_obs
   FROM prep
  ORDER BY prep.length_rank, prep.sum_obs DESC, prep.rank
WITH DATA;

ALTER TABLE congestion.mergeshort_result3_ordered_v2
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.mergeshort_result3_ordered_v2 TO bdit_humans;
GRANT ALL ON TABLE congestion.mergeshort_result3_ordered_v2 TO natalie;

-- View: congestion.partitioned_corridors

-- DROP MATERIALIZED VIEW congestion.partitioned_corridors;

CREATE MATERIALIZED VIEW congestion.partitioned_corridors
TABLESPACE pg_default
AS
 WITH longer AS (
         SELECT route_corridor.corridor_id,
            route_corridor.start_vid,
            route_corridor.end_vid,
            route_corridor.link_set,
            route_corridor.length_set,
            route_corridor.length
           FROM congestion.route_corridor
             LEFT JOIN congestion.matched_corridors match USING (corridor_id)
          WHERE match.corridor_id IS NULL AND route_corridor.length > 250::double precision
        ), shorter_ones AS (
         SELECT route_corridor.corridor_id,
            0 AS id,
            route_corridor.link_set,
            route_corridor.length
           FROM congestion.route_corridor
             LEFT JOIN longer USING (corridor_id)
             LEFT JOIN congestion.matched_corridors USING (corridor_id)
          WHERE longer.corridor_id IS NULL AND matched_corridors.* IS NULL
        ), corrected AS (
         SELECT partition_simanneal_v3.segment_id AS corridor_id,
            partition_simanneal_v3.id,
            partition_simanneal_v3.link_set,
            partition_simanneal_v3.length
           FROM congestion.partition_simanneal_v3
        ), alla AS (
         SELECT shorter_ones.corridor_id,
            shorter_ones.id,
            shorter_ones.link_set,
            shorter_ones.length
           FROM shorter_ones
        UNION ALL
         SELECT corrected.corridor_id,
            corrected.id,
            corrected.link_set,
            corrected.length
           FROM corrected
        ), again AS (
         SELECT alla.corridor_id,
            20000 + row_number() OVER (ORDER BY alla.corridor_id, alla.id) AS segment_id,
            alla.link_set,
            alla.length
           FROM alla
        )
 SELECT a.corridor_id,
    a.segment_id,
    array_agg(routing_grid.link_dir) AS link_set,
    a.length
   FROM ( SELECT again.corridor_id,
            again.segment_id,
            unnest(again.link_set) AS unnest,
            again.length
           FROM again) a
     JOIN congestion.routing_grid ON routing_grid.id::numeric = a.unnest
  GROUP BY a.corridor_id, a.segment_id, a.length
WITH DATA;

ALTER TABLE congestion.partitioned_corridors
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.partitioned_corridors TO bdit_humans;
GRANT ALL ON TABLE congestion.partitioned_corridors TO natalie;

-- View: congestion.partitioned_segments

-- DROP MATERIALIZED VIEW congestion.partitioned_segments;

CREATE MATERIALIZED VIEW congestion.partitioned_segments
TABLESPACE pg_default
AS
 WITH temp AS (
         SELECT a.segment_id,
            a.start_vid,
            a.end_vid,
            a.link_set,
            a.length_set,
            sum(a.unnest) AS length
           FROM ( SELECT merged_segments.segment_id,
                    merged_segments.start_vid,
                    merged_segments.end_vid,
                    merged_segments.link_set,
                    merged_segments.length_set,
                    unnest(merged_segments.length_set) AS unnest
                   FROM congestion.merged_segments) a
          GROUP BY a.segment_id, a.start_vid, a.end_vid, a.link_set, a.length_set
        ), tempa AS (
         SELECT temp.link_set,
            temp.length
           FROM temp
          WHERE NOT (temp.segment_id IN ( SELECT DISTINCT test_pythonmerge_probs.segment_id
                   FROM congestion.test_pythonmerge_probs))
        UNION ALL
         SELECT test_pythonmerge_probs.link_set,
            test_pythonmerge_probs.length
           FROM congestion.test_pythonmerge_probs
        ), tempaa AS (
         SELECT a.segment_id,
            unnest(a.link_set) AS link_dir,
            a.length
           FROM ( SELECT row_number() OVER () AS segment_id,
                    tempa.link_set,
                    tempa.length
                   FROM tempa) a
        )
 SELECT tempaa.segment_id,
    array_agg(tempaa.link_dir) AS link_set,
    tempaa.length,
    st_linemerge(st_union(routing_grid.geom)) AS geom
   FROM tempaa
     JOIN congestion.routing_grid USING (link_dir)
  GROUP BY tempaa.segment_id, tempaa.length
WITH DATA;

ALTER TABLE congestion.partitioned_segments
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.partitioned_segments TO bdit_humans;
GRANT ALL ON TABLE congestion.partitioned_segments TO natalie;

-- View: congestion.partitioned_segments_v2

-- DROP MATERIALIZED VIEW congestion.partitioned_segments_v2;

CREATE MATERIALIZED VIEW congestion.partitioned_segments_v2
TABLESPACE pg_default
AS
 WITH results AS (
         SELECT merged_segments_v2.link_set,
            merged_segments_v2.length_set,
            merged_segments_v2.length
           FROM ( SELECT merged_segments_v2_1.segment_id
                   FROM congestion.merged_segments_v2 merged_segments_v2_1
                EXCEPT
                 SELECT DISTINCT partition_result.segment_id
                   FROM congestion.partition_result) a_1
             JOIN congestion.merged_segments_v2 USING (segment_id)
        UNION ALL
         SELECT partition_result.link_set,
            partition_result.length_set,
            partition_result.length
           FROM congestion.partition_result
        ), segments AS (
         SELECT row_number() OVER () AS segment_id,
            results.link_set,
            results.length_set,
            results.length
           FROM results
        )
 SELECT a.segment_id,
    a.link_set,
    a.length_set,
    a.length,
    st_linemerge(st_union(routing_grid.geom)) AS geom
   FROM ( SELECT segments.segment_id,
            segments.link_set,
            unnest(segments.link_set) AS links,
            segments.length_set,
            segments.length
           FROM segments) a
     JOIN congestion.routing_grid ON a.links = routing_grid.id
  GROUP BY a.segment_id, a.link_set, a.length_set, a.length
WITH DATA;

ALTER TABLE congestion.partitioned_segments_v2
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.partitioned_segments_v2 TO bdit_humans;
GRANT ALL ON TABLE congestion.partitioned_segments_v2 TO natalie;

-- View: congestion.partitioned_segments_v3

-- DROP MATERIALIZED VIEW congestion.partitioned_segments_v3;

CREATE MATERIALIZED VIEW congestion.partitioned_segments_v3
TABLESPACE pg_default
AS
 WITH bad_anneal AS (
         SELECT DISTINCT a.segment_id AS bad_segment_id
           FROM ( SELECT partition_simanneal.segment_id,
                    unnest(partition_simanneal.length_set) AS unnest,
                    partition_simanneal.length
                   FROM congestion.partition_simanneal) a
          WHERE a.length < 100::double precision
        ), good_anneal AS (
         SELECT partition_simanneal.segment_id,
            partition_simanneal.id,
            partition_simanneal.link_set,
            partition_simanneal.length_set,
            partition_simanneal.length
           FROM congestion.partition_simanneal
             LEFT JOIN bad_anneal ON bad_anneal.bad_segment_id = partition_simanneal.segment_id
          WHERE bad_anneal.bad_segment_id IS NULL
        ), better AS (
         SELECT partition_all_possibility.segment_id,
            partition_all_possibility.id,
            partition_all_possibility.link_set,
            partition_all_possibility.length_set,
            partition_all_possibility.length
           FROM congestion.partition_all_possibility
        UNION ALL
         SELECT good_anneal.segment_id,
            good_anneal.id,
            good_anneal.link_set,
            good_anneal.length_set,
            good_anneal.length
           FROM good_anneal
        ), final AS (
         SELECT better.link_set,
            better.length_set,
            better.length
           FROM better
        UNION ALL
         SELECT partition_result.link_set,
            partition_result.length_set,
            partition_result.length
           FROM congestion.partition_result
             LEFT JOIN better USING (segment_id)
          WHERE better.segment_id IS NULL
        UNION ALL
         SELECT merged_segments_v2.link_set,
            merged_segments_v2.length_set,
            merged_segments_v2.length
           FROM congestion.merged_segments_v2
          WHERE merged_segments_v2.length <= 200
        )
 SELECT row_number() OVER () AS segment_id,
    final.link_set,
    final.length_set,
    final.length
   FROM final
WITH DATA;

ALTER TABLE congestion.partitioned_segments_v3
    OWNER TO natalie;

COMMENT ON MATERIALIZED VIEW congestion.partitioned_segments_v3
    IS 'grid 3.0, created using a combination of greedy partitions, best possible group partitions, and simulated annealing';

GRANT SELECT ON TABLE congestion.partitioned_segments_v3 TO bdit_humans;
GRANT ALL ON TABLE congestion.partitioned_segments_v3 TO natalie;

-- View: congestion.partitioned_segments_v4

-- DROP MATERIALIZED VIEW congestion.partitioned_segments_v4;

CREATE MATERIALIZED VIEW congestion.partitioned_segments_v4
TABLESPACE pg_default
AS
 WITH all_results AS (
         SELECT partition_all_possibility_v5.segment_id,
            partition_all_possibility_v5.link_set,
            partition_all_possibility_v5.id,
            partition_all_possibility_v5.length_set,
            partition_all_possibility_v5.length,
            'v4'::text AS version,
            abs(200::double precision - partition_all_possibility_v5.length) AS diff
           FROM congestion.partition_all_possibility_v5
        UNION
         SELECT partition_all_possibility_v4.segment_id,
            partition_all_possibility_v4.link_set,
            partition_all_possibility_v4.id,
            partition_all_possibility_v4.length_set,
            partition_all_possibility_v4.length,
            'v4'::text AS version,
            abs(200::double precision - partition_all_possibility_v4.length) AS diff
           FROM congestion.partition_all_possibility_v4
        UNION
         SELECT partition_all_possibility_v3.segment_id,
            partition_all_possibility_v3.link_set,
            partition_all_possibility_v3.id,
            partition_all_possibility_v3.length_set,
            partition_all_possibility_v3.length,
            'v3'::text AS version,
            abs(200::double precision - partition_all_possibility_v3.length) AS diff
           FROM congestion.partition_all_possibility_v3
        UNION
         SELECT partition_all_possibility_v2.segment_id,
            partition_all_possibility_v2.link_set,
            partition_all_possibility_v2.id,
            partition_all_possibility_v2.length_set,
            partition_all_possibility_v2.length,
            'v2'::text AS version,
            abs(200::double precision - partition_all_possibility_v2.length) AS diff
           FROM congestion.partition_all_possibility_v2
        UNION
         SELECT partition_result.segment_id,
            partition_result.link_set,
            partition_result.id,
            partition_result.length_set,
            partition_result.length,
            'greedy'::text AS version,
            abs(200 - partition_result.length) AS diff
           FROM congestion.partition_result
        UNION
         SELECT partition_simanneal.segment_id,
            partition_simanneal.link_set,
            partition_simanneal.id,
            partition_simanneal.length_set,
            partition_simanneal.length,
            'anneal'::text AS version,
            abs(200::double precision - partition_simanneal.length) AS diff
           FROM congestion.partition_simanneal
        UNION
         SELECT partition_simanneal_v2.segment_id,
            partition_simanneal_v2.link_set,
            partition_simanneal_v2.id,
            partition_simanneal_v2.length_set,
            partition_simanneal_v2.length,
            'anneal2'::text AS version,
            abs(200::double precision - partition_simanneal_v2.length) AS diff
           FROM congestion.partition_simanneal_v2
  ORDER BY 6, 1, 3
        ), evaluate AS (
         SELECT row_number() OVER (PARTITION BY all_results.segment_id ORDER BY (sum(all_results.diff))) AS rank,
            all_results.segment_id,
            all_results.version,
            sum(all_results.diff) AS error
           FROM all_results
          GROUP BY all_results.segment_id, all_results.version
        ), selection AS (
         SELECT evaluate.segment_id,
            all_results.id,
            all_results.link_set,
            all_results.length_set,
            all_results.length,
            evaluate.version
           FROM evaluate
             JOIN all_results USING (segment_id, version)
          WHERE evaluate.rank = 1
          ORDER BY all_results.length DESC
        ), alla AS (
         SELECT merged_segments_v2.segment_id,
            0 AS id,
            merged_segments_v2.link_set,
            merged_segments_v2.length_set,
            merged_segments_v2.length,
            'og'::text AS version
           FROM congestion.merged_segments_v2
             JOIN ( SELECT merged_segments_v2_1.segment_id
                   FROM congestion.merged_segments_v2 merged_segments_v2_1
                EXCEPT
                 SELECT selection.segment_id
                   FROM selection) a USING (segment_id)
        UNION
         SELECT selection.segment_id,
            selection.id,
            selection.link_set,
            selection.length_set,
            selection.length,
            selection.version
           FROM selection
  ORDER BY 1
        )
 SELECT row_number() OVER () AS segment_id,
    alla.link_set,
    alla.length_set,
    alla.length
   FROM alla
WITH DATA;

ALTER TABLE congestion.partitioned_segments_v4
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.partitioned_segments_v4 TO bdit_humans;
GRANT ALL ON TABLE congestion.partitioned_segments_v4 TO natalie;

-- View: congestion.route_int2int_nextbest

-- DROP MATERIALIZED VIEW congestion.route_int2int_nextbest;

CREATE MATERIALIZED VIEW congestion.route_int2int_nextbest
TABLESPACE pg_default
AS
 WITH temp AS (
         SELECT route_int2int.segment_id,
            route_int2int.start_vid,
            route_int2int.end_vid,
            route_int2int.link_set,
            route_int2int.geom,
            st_length(st_transform(route_int2int.geom, 2952)) AS length,
            degrees(st_azimuth(st_startpoint(st_line_substring(route_int2int.geom, 0::double precision, 2::double precision / st_length(st_transform(route_int2int.geom, 2952)))), st_endpoint(st_line_substring(route_int2int.geom, 0::double precision, 2::double precision / st_length(st_transform(route_int2int.geom, 2952)))))) AS start_subsec,
            degrees(st_azimuth(st_startpoint(st_line_substring(route_int2int.geom, (st_length(st_transform(route_int2int.geom, 2952)) - 2::double precision) / st_length(st_transform(route_int2int.geom, 2952)), 1::double precision)), st_endpoint(st_line_substring(route_int2int.geom, (st_length(st_transform(route_int2int.geom, 2952)) - 2::double precision) / st_length(st_transform(route_int2int.geom, 2952)), 1::double precision)))) AS end_subsec
           FROM congestion.route_int2int_distinct route_int2int
        ), cal AS (
         SELECT pot.segment_id AS pot_seg,
            pot.start_vid AS pot_start_vid,
            pot.end_vid AS pot_end_vid,
            pot.link_set AS pot_link_set,
            pot.geom AS pot_geom,
            st_length(st_transform(pot.geom, 2952)) AS pot_length,
                CASE
                    WHEN pot.start_vid = temp.end_vid THEN degrees(st_azimuth(st_startpoint(st_line_substring(pot.geom, 0::double precision, 2::double precision / st_length(st_transform(pot.geom, 2952)))), st_endpoint(st_line_substring(pot.geom, 0::double precision, 2::double precision / st_length(st_transform(pot.geom, 2952))))))
                    ELSE degrees(st_azimuth(st_startpoint(st_line_substring(pot.geom, (st_length(st_transform(pot.geom, 2952)) - 2::double precision) / st_length(st_transform(pot.geom, 2952)), 1::double precision)), st_endpoint(st_line_substring(pot.geom, (st_length(st_transform(pot.geom, 2952)) - 2::double precision) / st_length(st_transform(pot.geom, 2952)), 1::double precision))))
                END AS pot_az,
            temp.segment_id,
            temp.start_vid,
            temp.end_vid,
            temp.link_set,
            temp.geom,
            temp.length,
            temp.start_subsec,
            temp.end_subsec
           FROM congestion.route_int2int_distinct pot,
            temp
          WHERE (pot.start_vid = temp.end_vid OR pot.end_vid = temp.start_vid) AND NOT (pot.start_vid = temp.end_vid AND pot.end_vid = temp.start_vid)
        ), next_best AS (
         SELECT cal.pot_seg,
            cal.pot_start_vid,
            cal.pot_end_vid,
            cal.pot_link_set,
            cal.pot_geom,
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END AS ang_diff,
            cal.pot_length,
            cal.length,
            cal.segment_id,
            cal.start_vid,
            cal.end_vid,
            cal.link_set,
            cal.geom
           FROM cal
          ORDER BY (
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END), cal.length
        )
 SELECT row_number() OVER (PARTITION BY a.segment_id, a.length, a.start_vid, a.end_vid, a.link_set, a.geom ORDER BY b.ang_diff) AS rank,
    a.segment_id,
    a.start_vid,
    a.end_vid,
    a.link_set,
    a.length,
    a.geom,
    b.pot_seg,
    b.pot_start_vid,
    b.pot_end_vid,
    b.pot_link_set,
    b.pot_length,
    b.pot_geom,
    b.ang_diff
   FROM temp a
     LEFT JOIN LATERAL ( SELECT next_best.pot_seg,
            next_best.pot_length,
            next_best.pot_start_vid,
            next_best.pot_end_vid,
            next_best.pot_link_set,
            next_best.pot_geom,
            next_best.ang_diff
           FROM next_best
          WHERE a.start_vid = next_best.start_vid AND a.end_vid = next_best.end_vid
         LIMIT 2) b ON true
WITH DATA;

ALTER TABLE congestion.route_int2int_nextbest
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.route_int2int_nextbest TO bdit_humans;
GRANT ALL ON TABLE congestion.route_int2int_nextbest TO natalie;

-- View: congestion.route_nextbest_ordered

-- DROP MATERIALIZED VIEW congestion.route_nextbest_ordered;

CREATE MATERIALIZED VIEW congestion.route_nextbest_ordered
TABLESPACE pg_default
AS
 WITH rank AS (
         SELECT a.start_vid,
            a.end_vid,
            sum(linkdir_obs.total_count) AS sum
           FROM ( SELECT DISTINCT route_nextbest_1.start_vid,
                    route_nextbest_1.end_vid,
                    unnest(route_nextbest_1.link_set) AS unnest
                   FROM congestion.route_int2int_nextbest route_nextbest_1) a
             JOIN congestion.routing_grid ON a.unnest = routing_grid.link_dir
             JOIN congestion.linkdir_obs USING (link_dir)
          GROUP BY a.start_vid, a.end_vid
        ), prep AS (
         SELECT
                CASE
                    WHEN route_int2int_nextbest.length < 50::double precision THEN 1
                    WHEN route_int2int_nextbest.length >= 50::double precision AND route_int2int_nextbest.length < 100::double precision THEN 2
                    WHEN route_int2int_nextbest.length >= 100::double precision AND route_int2int_nextbest.length <= 200::double precision THEN 3
                    ELSE 4
                END AS length_rank,
            route_int2int_nextbest.start_vid,
            route_int2int_nextbest.end_vid,
            route_int2int_nextbest.rank,
            route_int2int_nextbest.segment_id,
            route_int2int_nextbest.link_set,
            route_int2int_nextbest.length,
            route_int2int_nextbest.geom,
            route_int2int_nextbest.pot_seg,
            route_int2int_nextbest.pot_start_vid,
            route_int2int_nextbest.pot_end_vid,
            route_int2int_nextbest.pot_link_set,
            route_int2int_nextbest.pot_length,
            route_int2int_nextbest.pot_geom,
            route_int2int_nextbest.ang_diff,
            rank.sum
           FROM congestion.route_int2int_nextbest
             JOIN rank USING (start_vid, end_vid)
        )
 SELECT prep.start_vid,
    prep.end_vid,
    prep.rank,
    prep.segment_id,
    prep.link_set,
    prep.length,
    prep.geom,
    prep.pot_seg,
    prep.pot_start_vid,
    prep.pot_end_vid,
    prep.pot_link_set,
    prep.pot_length,
    prep.pot_geom,
    prep.ang_diff,
    prep.sum
   FROM prep
  ORDER BY prep.length_rank, prep.sum DESC, prep.rank
WITH DATA;

ALTER TABLE congestion.route_nextbest_ordered
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.route_nextbest_ordered TO bdit_humans;
GRANT ALL ON TABLE congestion.route_nextbest_ordered TO natalie;

-- View: congestion.route_nextbest_ordered_v2

-- DROP MATERIALIZED VIEW congestion.route_nextbest_ordered_v2;

CREATE MATERIALIZED VIEW congestion.route_nextbest_ordered_v2
TABLESPACE pg_default
AS
 WITH temp AS (
         SELECT route_int2int.segment_id,
            route_int2int.start_vid,
            route_int2int.end_vid,
            route_int2int.link_set,
            route_int2int.length_set,
            route_int2int.geom,
            route_int2int.length,
            degrees(st_azimuth(st_startpoint(st_linesubstring(route_int2int.geom, 0::double precision, 2::double precision / route_int2int.length)), st_endpoint(st_linesubstring(route_int2int.geom, 0::double precision, 2::double precision / route_int2int.length)))) AS start_subsec,
            degrees(st_azimuth(st_startpoint(st_linesubstring(route_int2int.geom, (route_int2int.length - 2::double precision) / route_int2int.length, 1::double precision)), st_endpoint(st_linesubstring(route_int2int.geom, (route_int2int.length - 2::double precision) / route_int2int.length, 1::double precision)))) AS end_subsec
           FROM ( SELECT route_int2int3.segment_id,
                    route_int2int3.start_vid,
                    route_int2int3.end_vid,
                    route_int2int3.link_set,
                    route_int2int3.length_set,
                    route_int2int3.geom,
                    route_int2int3.length
                   FROM congestion.route_int2int3
                UNION ALL
                 SELECT route_int2int_missing.segment_id,
                    route_int2int_missing.start_vid,
                    route_int2int_missing.end_vid,
                    route_int2int_missing.link_set,
                    route_int2int_missing.length_set,
                    route_int2int_missing.geom,
                    route_int2int_missing.length
                   FROM congestion.route_int2int_missing) route_int2int
        ), cal AS (
         SELECT pot.segment_id AS pot_seg,
            pot.start_vid AS pot_start_vid,
            pot.end_vid AS pot_end_vid,
            pot.link_set AS pot_link_set,
            pot.length_set AS pot_length_set,
            pot.geom AS pot_geom,
            pot.length AS pot_length,
                CASE
                    WHEN pot.start_vid = temp.end_vid THEN degrees(st_azimuth(st_startpoint(st_linesubstring(pot.geom, 0::double precision, 2::double precision / pot.length)), st_endpoint(st_linesubstring(pot.geom, 0::double precision, 2::double precision / pot.length))))
                    ELSE degrees(st_azimuth(st_startpoint(st_linesubstring(pot.geom, (pot.length - 2::double precision) / pot.length, 1::double precision)), st_endpoint(st_linesubstring(pot.geom, (pot.length - 2::double precision) / pot.length, 1::double precision))))
                END AS pot_az,
            temp.segment_id,
            temp.start_vid,
            temp.end_vid,
            temp.link_set,
            temp.length_set,
            temp.geom,
            temp.length,
            temp.start_subsec,
            temp.end_subsec
           FROM ( SELECT route_int2int3.segment_id,
                    route_int2int3.start_vid,
                    route_int2int3.end_vid,
                    route_int2int3.link_set,
                    route_int2int3.length_set,
                    route_int2int3.geom,
                    route_int2int3.length
                   FROM congestion.route_int2int3
                UNION ALL
                 SELECT route_int2int_missing.segment_id,
                    route_int2int_missing.start_vid,
                    route_int2int_missing.end_vid,
                    route_int2int_missing.link_set,
                    route_int2int_missing.length_set,
                    route_int2int_missing.geom,
                    route_int2int_missing.length
                   FROM congestion.route_int2int_missing) pot,
            temp
          WHERE (pot.start_vid = temp.end_vid OR pot.end_vid = temp.start_vid) AND NOT (pot.start_vid = temp.end_vid AND pot.end_vid = temp.start_vid)
        ), next_best AS (
         SELECT cal.pot_seg,
            cal.pot_start_vid,
            cal.pot_end_vid,
            cal.pot_link_set,
            cal.pot_length_set,
            cal.pot_geom,
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END AS ang_diff,
            cal.pot_length,
            cal.length,
            cal.segment_id,
            cal.start_vid,
            cal.end_vid,
            cal.link_set,
            cal.length_set,
            cal.geom
           FROM cal
          ORDER BY (
                CASE
                    WHEN cal.pot_start_vid = cal.end_vid THEN abs(cal.pot_az - cal.end_subsec)
                    WHEN cal.pot_end_vid = cal.start_vid THEN abs(cal.pot_az - cal.start_subsec)
                    ELSE NULL::double precision
                END), cal.length
        ), selections AS (
         SELECT row_number() OVER (PARTITION BY a.segment_id, a.length, a.start_vid, a.end_vid, a.link_set, a.geom ORDER BY b.ang_diff) AS rank,
            a.segment_id,
            a.start_vid,
            a.end_vid,
            a.link_set,
            a.length_set,
            a.length,
            a.geom,
            b.pot_seg,
            b.pot_start_vid,
            b.pot_end_vid,
            b.pot_link_set,
            b.pot_length_set,
            b.pot_length,
            b.pot_geom,
            b.ang_diff
           FROM temp a
             LEFT JOIN LATERAL ( SELECT next_best.pot_seg,
                    next_best.pot_length,
                    next_best.pot_start_vid,
                    next_best.pot_end_vid,
                    next_best.pot_link_set,
                    next_best.pot_length_set,
                    next_best.pot_geom,
                    next_best.ang_diff
                   FROM next_best
                  WHERE a.start_vid = next_best.start_vid AND a.end_vid = next_best.end_vid
                 LIMIT 2) b ON true
        ), rank AS (
         SELECT a.start_vid,
            a.end_vid,
            sum(linkdir_obs.total_count) AS sum
           FROM ( SELECT route_int2int3.segment_id,
                    route_int2int3.start_vid,
                    route_int2int3.end_vid,
                    route_int2int3.link_set,
                    route_int2int3.length_set,
                    route_int2int3.geom,
                    route_int2int3.length,
                    unnest(route_int2int3.link_set) AS unnest
                   FROM congestion.route_int2int3
                UNION ALL
                 SELECT route_int2int_missing.segment_id,
                    route_int2int_missing.start_vid,
                    route_int2int_missing.end_vid,
                    route_int2int_missing.link_set,
                    route_int2int_missing.length_set,
                    route_int2int_missing.geom,
                    route_int2int_missing.length,
                    unnest(route_int2int_missing.link_set) AS unnest
                   FROM congestion.route_int2int_missing) a
             JOIN congestion.routing_grid ON a.unnest = routing_grid.id
             JOIN congestion.linkdir_obs USING (link_dir)
          GROUP BY a.start_vid, a.end_vid
        ), prep AS (
         SELECT
                CASE
                    WHEN selections.length < 50::double precision THEN 1
                    WHEN selections.length >= 50::double precision AND selections.length < 100::double precision THEN 2
                    WHEN selections.length >= 100::double precision AND selections.length <= 200::double precision THEN 3
                    ELSE 4
                END AS length_rank,
            selections.start_vid,
            selections.end_vid,
            selections.rank,
            selections.segment_id,
            selections.link_set,
            selections.length_set,
            selections.length,
            selections.geom,
            selections.pot_seg,
            selections.pot_start_vid,
            selections.pot_end_vid,
            selections.pot_link_set,
            selections.pot_length_set,
            selections.pot_length,
            selections.pot_geom,
            selections.ang_diff,
            rank.sum
           FROM selections
             JOIN rank USING (start_vid, end_vid)
        )
 SELECT prep.start_vid,
    prep.end_vid,
    prep.rank,
    prep.segment_id,
    prep.link_set,
    prep.length_set,
    prep.length,
    prep.geom,
    prep.pot_seg,
    prep.pot_start_vid,
    prep.pot_end_vid,
    prep.pot_link_set,
    prep.pot_length_set,
    prep.pot_length,
    prep.pot_geom,
    prep.ang_diff,
    prep.sum
   FROM prep
  ORDER BY prep.length_rank, prep.sum DESC, prep.rank
WITH DATA;

ALTER TABLE congestion.route_nextbest_ordered_v2
    OWNER TO natalie;

GRANT SELECT ON TABLE congestion.route_nextbest_ordered_v2 TO bdit_humans;
GRANT ALL ON TABLE congestion.route_nextbest_ordered_v2 TO natalie;

