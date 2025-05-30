CREATE OR REPLACE FUNCTION congestion.create_yearly_daily_table(yyyy text)
RETURNS void
LANGUAGE plpgsql
COST 100
VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE 
	startdate DATE;
	enddate DATE;
	basetablename TEXT := 'network_segments_daily_';
	tablename TEXT;
	yyyymm TEXT;

	BEGIN
    FOR mm IN 01..12 LOOP
        startdate:= to_date(yyyy||'-'||mm||'-01', 'YYYY-MM-DD');
		enddate:= startdate + INTERVAL '1 month';
        IF mm < 10 THEN
            yyyymm:= yyyy||'0'||mm;
        ELSE
            yyyymm:= yyyy||''||mm;
        END IF;
        tablename:= basetablename||yyyymm;
    	EXECUTE format($$CREATE TABLE congestion.%I 
                     PARTITION OF congestion.network_segments_daily
                     FOR VALUES FROM  (%L) TO (%L);
                     CREATE INDEX ON congestion.%I  (segment_id);
                     CREATE INDEX ON congestion.%I  (hr);
                     CREATE INDEX ON congestion.%I  (dt);
                     ALTER TABLE congestion.%I ADD UNIQUE(segment_id, hr, dt);
                     ALTER TABLE congestion.%I OWNER TO congestion_admins;
                     $$
                     , tablename, startdate, enddate, tablename, tablename, tablename, tablename, tablename);
	END LOOP;
END;
$BODY$;

ALTER FUNCTION congestion.create_yearly_daily_table(text)
OWNER TO congestion_admins;

COMMENT ON FUNCTION congestion.create_yearly_daily_table(text)
IS 'Function to create yearly partitioned table for network_segments_daily. Scheduled to execute at the end of the year with EOY maintanence airflow DAG.';
