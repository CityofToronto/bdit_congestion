-- FUNCTION: congestion.create_yyyy_volumes_partition(text, integer, text)

-- DROP FUNCTION IF EXISTS congestion.create_yyyy_volumes_partition(text, integer, text);

CREATE OR REPLACE FUNCTION congestion.create_yyyy_volumes_partition(
	year_ integer)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE
	year_table TEXT := 'network_segments_daily_spd_'||year_::text;
	startdate DATE := (year_::text || '-01-01')::date;
	enddate DATE := ((year_+1)::text || '-01-01')::date;

BEGIN

    EXECUTE FORMAT($$
        CREATE TABLE IF NOT EXISTS congestion.%I
        PARTITION OF congestion.network_segments_daily_spd
        FOR VALUES FROM (%L) TO (%L)
        PARTITION BY RANGE (dt);
        ALTER TABLE IF EXISTS congestion.%I OWNER TO congestion_admins;
        GRANT TRIGGER, SELECT, INSERT ON TABLE congestion.%I TO congestion_bot;
        GRANT SELECT ON TABLE congestion.%I TO bdit_humans;
        $$,
        year_table,
        startdate,
        enddate,
        year_table,
        year_table,
        year_table
    );

END;
$BODY$;

ALTER FUNCTION congestion.create_yyyy_volumes_partition(integer)
    OWNER TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.create_yyyy_volumes_partition(integer) TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.create_yyyy_volumes_partition(integer) TO congestion_bot;

REVOKE ALL ON FUNCTION congestion.create_yyyy_volumes_partition(integer) FROM PUBLIC;

COMMENT ON FUNCTION congestion.create_yyyy_volumes_partition(integer)
    IS 'Create a new year partition under the parent table network_segment_daily_spd.
Only to be used for congestion network daily aggregated speed table. ';
