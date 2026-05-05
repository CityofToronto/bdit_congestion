-- Usage
-- SELECT congestion.create_centreline_partition('24_4'::text, 'temp_centreline_lookup_24_4'::text);

CREATE OR REPLACE FUNCTION congestion.create_centreline_partition(
    ver text,
    input_table text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    partition_table text := format('congestion_centreline_%s', ver);
BEGIN
    -- Create partition table
    EXECUTE format(
        'CREATE TABLE congestion.%I AS
         SELECT segment_id,
                streetname,
                from_int,
                from_int_desc,
				to_int,
                to_int_desc,
				centreline_ids,
				centreline_uids,
				geom,
                %L AS ver_id
         FROM congestion.%I
         ORDER BY segment_id;',
        partition_table, ver, input_table
    );

    -- Add check constraint (constraint name = ver)
    EXECUTE format(
        'ALTER TABLE congestion.%I
         ADD CONSTRAINT %I CHECK (ver_id = %L);',
        partition_table, ver, ver
    );

    -- Attach partition
    EXECUTE format(
        'ALTER TABLE congestion.congestion_centreline
         ATTACH PARTITION congestion.%I
         FOR VALUES IN (%L);',
        partition_table, ver
    );
END;
$$;

ALTER FUNCTION congestion.create_centreline_partition(text, text)
    OWNER TO congestion_admins;
