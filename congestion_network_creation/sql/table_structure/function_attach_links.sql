-- Usage
-- SELECT congestion.create_links_partition('24_4'::text, 'temp_network_segments_24_4'::text); 
CREATE OR REPLACE FUNCTION congestion.create_links_partition(
    ver text,
    input_table text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    partition_table text := format('congestion_links_%s', ver);
BEGIN
    -- Create partition table
    EXECUTE format(
        'CREATE TABLE congestion.%I AS
         SELECT segment_id,
                start_vid,
                end_vid,
                link_dir,
                length,
                geom,
                %L AS ver_id
         FROM congestion.%I
         ORDER BY segment_id, link_dir;',
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
        'ALTER TABLE congestion.congestion_links
         ATTACH PARTITION congestion.%I
         FOR VALUES IN (%L);',
        partition_table, ver
    );
END;
$$;

ALTER FUNCTION congestion.create_links_partition(text, text)
    OWNER TO congestion_admins;
