-- one of the materialized view for merging, includes ranking and reordering for merging purposes

CREATE MATERIALIZED VIEW congestion.route_nextbest_ordered_v2

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


