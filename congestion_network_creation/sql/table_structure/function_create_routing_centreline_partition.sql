CREATE OR REPLACE FUNCTION gis_core.create_routing_centreline_partition(ver text)
RETURNS void
LANGUAGE plpgsql
AS $$ --routing_centreline_directional_higher_rc
DECLARE
    partition_table text := format('routing_centreline_directional_higher_rc_%s', ver);
	centreline_table text := format('centreline_%s', ver);
BEGIN
    -- Create partition table
    EXECUTE format(
		 'CREATE TABLE gis_core.%I AS
			 SELECT dup.centreline_id,
	    concat(row_number() OVER (), dup.dir)::bigint AS id,
		centreline_id::text||dir_text AS centreline_uid,
	    dup.source,
	    dup.target,
	    dup.cost_length,
	    dup.geom,
		%L AS version_date
   		FROM ( SELECT centreline.centreline_id,
            centreline.from_intersection_id AS source,
            centreline.to_intersection_id AS target,
            centreline.shape_length AS cost_length,
            centreline.geom,
            0 AS dir,
			''T'' AS dir_text
           FROM gis_core.%I centreline
          WHERE centreline.oneway_dir_code >= 0 AND (centreline.feature_code_desc = ANY (ARRAY[''Collector''::text, ''Major Arterial''::text, ''Expressway''::text, ''Major Arterial Ramp''::text, ''Minor Arterial''::text, ''Expressway Ramp''::text, ''Minor Arterial Ramp''::text, ''Pending''::text]))
        UNION
         SELECT centreline.centreline_id,
            centreline.to_intersection_id AS source,
            centreline.from_intersection_id AS target,
            centreline.shape_length AS cost_length,
            st_reverse(centreline.geom) AS geom,
            1 AS dir,
			''F'' AS dir_text
           FROM gis_core.%I centreline
          WHERE centreline.oneway_dir_code <= 0 AND (centreline.feature_code_desc = ANY (ARRAY[''Collector''::text, ''Major Arterial''::text, ''Expressway''::text, ''Major Arterial Ramp''::text, ''Minor Arterial''::text, ''Expressway Ramp''::text, ''Minor Arterial Ramp''::text, ''Pending''::text]))) dup;
',
        partition_table, ver, centreline_table, centreline_table
    );

    -- Add check constraint (constraint name = ver)
    EXECUTE format(
        'ALTER TABLE gis_core.%I
         ADD CONSTRAINT %I CHECK (version_date = %L);',
        partition_table, ver, ver
    );

    -- Attach partition
    EXECUTE format(
        'ALTER TABLE gis_core.routing_centreline_directional_higher_rc
         ATTACH PARTITION gis_core.%I
         FOR VALUES IN (%L);',
        partition_table, ver
    );
END;
$$;

ALTER FUNCTION gis_core.create_routing_centreline_partition(text)
    OWNER TO gis_admins;
