-- Usage
-- SELECT congestion.create_nodes_lookup_partition('24_4'::text, 'network_int_px_24_4'::text);

CREATE OR REPLACE FUNCTION congestion.create_nodes_lookup_partition(
    ver text,
    input_table text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    partition_table text := format('congestion_nodes_lookup_%s', ver);
BEGIN
    -- Create partition table
    EXECUTE format(
        'CREATE TABLE congestion.%I AS
         SELECT node_id,
                intersection_id,
				px,
				intersection_desc,
				highest_order_feature,
				node_geom,
				int_geom,
                %L AS ver_id
         FROM congestion.%I
         ORDER BY node_id;',
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
        'ALTER TABLE congestion.congestion_nodes_lookup
         ATTACH PARTITION congestion.%I
         FOR VALUES IN (%L);',
        partition_table, ver
    );
END;
$$;

ALTER FUNCTION congestion.create_nodes_lookup_partition(text, text)
    OWNER TO congestion_admins;

CREATE INDEX ON congestion.congestion_nodes_lookup (ver_id);