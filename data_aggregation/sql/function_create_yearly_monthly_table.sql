CREATE OR REPLACE FUNCTION congestion.create_yearly_monthly_table(yyyy text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE 
	startdate DATE;
	enddate DATE;
	basetablename TEXT := 'network_segments_monthly';
	tablename TEXT;
BEGIN

	startdate:= to_date(yyyy||'-01-01', 'YYYY-MM-DD');
	enddate:= startdate + INTERVAL '1 year';
	tablename:= basetablename||yyyy;
    EXECUTE format($$CREATE TABLE congestion.%I 
                     PARTITION OF congestion.network_segments_monthly
                     FOR VALUES FROM  (%L) TO (%L);
                     CREATE INDEX ON congestion.%I  (segment_id);
                     CREATE INDEX ON congestion.%I  (hr);
                     CREATE INDEX ON congestion.%I  (mth);
                     ALTER TABLE congestion.%I ADD UNIQUE(segment_id, hr, mth, day_type);
                     ALTER TABLE congestion.%I OWNER TO congestion_admins;
                     $$
                     , tablename, startdate, enddate, tablename, tablename, tablename, tablename, tablename);
END;
$BODY$;

ALTER FUNCTION congestion.create_yearly_monthly_table(text)
    OWNER TO congestion_admins;

COMMENT ON FUNCTION congestion.create_yearly_monthly_table(text)
    IS 'Function to create yearly partitioned table for network_segments_monthly. Scheduled to execute at the end of the year with EOY maintanence airflow DAG.';
