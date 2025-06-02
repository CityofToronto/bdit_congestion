-- FUNCTION: congestion.create_yyyymm_partitions(text, integer, integer)

-- DROP FUNCTION IF EXISTS congestion.create_yyyymm_partitions(text, integer, integer);

CREATE OR REPLACE FUNCTION congestion.create_yyyymm_partitions(
	year_ integer,
	mm_ integer)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE
	year_table TEXT := 'network_segments_daily_spd_'||year_::text;
    start_mm DATE;
    end_mm DATE;
	month_table TEXT;
    mm_pad TEXT;

BEGIN
    mm_pad:=lpad(mm_::text, 2, '0');
    start_mm:= to_date(year_::text||'-'||mm_pad||'-01', 'YYYY-MM-DD');
    end_mm:= start_mm + INTERVAL '1 month';
    month_table:= year_table||mm_pad;
    EXECUTE FORMAT($$
            CREATE TABLE IF NOT EXISTS congestion.%I
            PARTITION OF congestion.%I
            FOR VALUES FROM (%L) TO (%L);
            ALTER TABLE IF EXISTS congestion.%I OWNER TO congestion_admins;
            GRANT SELECT ON TABLE congestion.%I TO bdit_humans;
            GRANT SELECT, INSERT, UPDATE ON TABLE congestion.%I TO congestion_bot;
        $$,
        month_table,
        year_table,
        start_mm,
        end_mm,
        month_table,
        month_table,
        month_table
    );
END;
$BODY$;

ALTER FUNCTION congestion.create_yyyymm_partitions(integer, integer)
    OWNER TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.create_yyyymm_partitions(integer, integer) TO PUBLIC;

GRANT EXECUTE ON FUNCTION congestion.create_yyyymm_partitions(integer, integer) TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.create_yyyymm_partitions(integer, integer) TO congestion_bot;

COMMENT ON FUNCTION congestion.create_yyyymm_partitions(integer, integer)
    IS '''Create a new month partition under the parent year table network_segments_daily_spd''';
