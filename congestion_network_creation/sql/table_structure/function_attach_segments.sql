-- Usage
-- SELECT congestion.create_segment_partition('24_4'::text, 'temp_network_segments_24_4'::text);

CREATE OR REPLACE FUNCTION congestion.create_segment_partition(
    ver text,
    input_table text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    partition_table text := format('congestion_segments_%s', ver);
BEGIN

    -- Create partition table of input table, and ver_no
    EXECUTE format(
        'CREATE TABLE congestion.%I AS
         SELECT segment_id,
                start_vid,
                end_vid,
                total_length,
                dir::text,
                highway,
                geom,
                %L AS ver_id
         FROM congestion.%I
         ORDER BY segment_id;',
        partition_table, ver, input_table
    );

    -- Add check constraint
    EXECUTE format(
        'ALTER TABLE congestion.%I
         ADD CONSTRAINT %I CHECK (ver_id = %L);',
        partition_table, ver, ver
    );

    -- Attach partition
    EXECUTE format(
        'ALTER TABLE congestion.congestion_segments
         ATTACH PARTITION congestion.%I
         FOR VALUES IN (%L);',
        partition_table, ver
    );
	
	-- Change owner
	EXECUTE format(
        'ALTER TABLE congestion.%I
         OWNER TO congestion_admins; 
		 GRANT SELECT ON TABLE congestion.%I TO bdit_humans; ',
        partition_table, partition_table
    );
END;
$$;

ALTER FUNCTION congestion.create_segment_partition(text, text)
    OWNER TO congestion_admins;

