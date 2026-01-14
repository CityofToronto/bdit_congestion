CREATE OR REPLACE FUNCTION congestion.create_yearly_tables(_yyyy text)
RETURNS void
LANGUAGE sql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

-- For network_segments_daily
SELECT congestion.create_yearly_daily_table(_yyyy);

-- For network_segments_monthly
SELECT congestion.create_yearly_daily_table(_yyyy);

-- For centreline_monthly
SELECT congestion.create_yearly_monthly_centreline_table(_yyyy);

$BODY$;

ALTER FUNCTION congestion.create_yearly_tables(_yyyy text)
OWNER TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.create_yearly_tables(_yyyy text) TO congestion_bot;

REVOKE ALL ON FUNCTION congestion.create_yearly_tables(_yyyy text) FROM public;

COMMENT ON FUNCTION congestion.create_yearly_tables(_yyyy text)
IS 'Function that runs three functions to create yearly tables.';
