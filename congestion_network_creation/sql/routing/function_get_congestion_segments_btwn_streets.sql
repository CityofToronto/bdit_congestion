
-- example:  select * FROM congestion.get_congestion_segments_btwn_streets('Yonge St', 'Eglinton Ave', 'Davisville Ave', '24_4')
CREATE OR REPLACE FUNCTION congestion.get_congestion_segments_btwn_streets(
    _main_street  text,
    _start_street text,
    _end_street   text,
    _ver_id       text
)
RETURNS TABLE (
     start_node integer,
	 end_node integer,
	 segment_list integer[],
	 length numeric,
	 geom geometry
)
LANGUAGE plpgsql
AS $$
DECLARE
    start_int_id bigint;
    end_int_id   bigint;
BEGIN
    -- Find the start intersection
    start_int_id :=
        (gis._get_intersection_id(
            _main_street,
            _start_street,
            1
        ))[3];

    IF start_int_id IS NULL THEN
        RAISE EXCEPTION
            'No intersection_id could be matched for "%" and "%".',
            _main_street,
            _start_street;
    END IF;

    -- Find the end intersection
    end_int_id :=
        (gis._get_intersection_id(
            _main_street,
            _end_street,
            1
        ))[3];

    IF end_int_id IS NULL THEN
        RAISE EXCEPTION
            'No intersection_id could be matched for "%" and "%".',
            _main_street,
            _end_street;
    END IF;

    RETURN QUERY
    SELECT *
    FROM congestion.get_congestion_segments_btwn_ints(
        start_int_id::int,
        end_int_id::int,
        _ver_id
    );

END;
$$;


ALTER FUNCTION congestion.get_congestion_segments_btwn_streets(text, text, text, text)
    OWNER TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.get_congestion_segments_btwn_streets(text, text, text, text) TO PUBLIC;

GRANT EXECUTE ON FUNCTION congestion.get_congestion_segments_btwn_streets(text, text, text, text) TO bdit_humans;

GRANT EXECUTE ON FUNCTION congestion.get_congestion_segments_btwn_streets(text, text, text, text) TO congestion_admins;

COMMENT ON FUNCTION congestion.get_congestion_segments_btwn_streets(text, text, text, text)
    IS 'Functions to input main, start and end street name and return the congestion segments.';
