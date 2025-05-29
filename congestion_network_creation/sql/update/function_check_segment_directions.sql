-- FUNCTION: congestion.check_segment_directions(text)

-- DROP FUNCTION IF EXISTS congestion.check_segment_directions(text);

CREATE OR REPLACE FUNCTION congestion.check_segment_directions(
    version_suffix text
)
RETURNS TABLE (
    segment_id bigint,
    start_vid bigint,
    end_vid bigint,
    proposed_source bigint,
    proposed_target bigint,
    o1_dir text,
    o2_dir text,
    o3_dir text,
    o4_dir text
)
LANGUAGE plpgsql
COST 100
VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
ROWS 1000

AS $BODY$

DECLARE 
	network_table TEXT := 'network_links_' || version_suffix;
	streets_table TEXT := 'routing_streets_' || version_suffix;
	sql TEXT;
BEGIN

RETURN QUERY  EXECUTE format($$
with some_order AS (
select segment_id, start_vid, end_vid, link_dir, source, target, 
case when start_vid = source then 1
when end_vid = target then 10
else null end as orders
            FROM congestion.%I nl
            INNER JOIN here.%I rs USING (link_dir)
        )

, first_n_last_link AS (
select segment_id, start_vid, end_vid, link_dir, source, target, orders
from some_order
where orders is not null
union 
select f_l.segment_id, f_l.start_vid, f_l.end_vid, f_l.link_dir, f_l.source, f_l.target, 2 AS orders
from some_order f_l
inner join (select * from some_order where orders = 1) nxt 
    on f_l.source = nxt.target and f_l.segment_id = nxt.segment_id
union
select f_l.segment_id, f_l.start_vid, f_l.end_vid, f_l.link_dir, f_l.source, f_l.target, 9 AS orders
from some_order f_l
inner join (select * from some_order where orders = 10) nxt 
    on f_l.target = nxt.source and f_l.segment_id = nxt.segment_id
where f_l.orders is null
order by segment_id, orders),
        with_dir AS (
            SELECT segment_id, start_vid, end_vid, a.source, a.target, gis.direction_from_line(a.geom) dir, orders, a.geom
from first_n_last_link
            INNER JOIN here.%I  a USING (link_dir)
        ), results AS (

            SELECT 
                o1.segment_id, o1.start_vid, o1.end_vid, 
                CASE 
                    WHEN o1.dir != o2.dir AND o2.dir IS NOT NULL THEN o1.target 
                    ELSE NULL 
                END AS proposed_source,
                CASE 
                    WHEN o3.dir != o4.dir AND o4.dir IS NOT NULL THEN o4.source 
                    ELSE NULL 
                END AS proposed_target,
                o1.dir AS o1_dir, o2.dir AS o2_dir, o3.dir AS o3_dir, o4.dir AS o4_dir
            FROM (SELECT * FROM with_dir WHERE orders = 1) o1
            LEFT JOIN (SELECT * FROM with_dir WHERE orders = 2) o2 USING (segment_id)
            LEFT JOIN (SELECT * FROM with_dir WHERE orders = 9) o3 USING (segment_id)
            LEFT JOIN (SELECT * FROM with_dir WHERE orders = 10) o4 USING (segment_id))

            SELECT segment_id, start_vid, end_vid, proposed_source::BIGINT, proposed_target::BIGINT, 
o1_dir::text, o2_dir::text, o3_dir::text, o4_dir::text from results
where proposed_source is not null or proposed_target is not null
    $$, network_table, streets_table, streets_table);

END;
$BODY$;

ALTER FUNCTION congestion.check_segment_directions(text)
OWNER TO congestion_admins;

COMMENT ON FUNCTION congestion.check_segment_directions(text)
IS 'Checks directionality inconsistencies in segment_id geometry';
