-- select gis_core.create_routing_restriction_partition('20260301')
CREATE OR REPLACE FUNCTION gis_core.create_routing_restriction_partition(ver text)
RETURNS void
LANGUAGE plpgsql
AS $$ --routing_centreline_directional_higher_rc
DECLARE
    partition_table text := format('centreline_routing_restrictions_higher_rc_%s', ver);
	centreline_table text := format('intersection_%s', ver);
	routing_table text := format('routing_centreline_directional_higher_rc_%s', ver); 
BEGIN
    -- Create partition table
    EXECUTE format(
		'CREATE TABLE gis_core.%I AS
		 SELECT DISTINCT ARRAY[s.id, e.id] AS path,
    		9999 AS cost, 
			%s::Text AS version_date
   			FROM gis_core.%I intersection_latest
     	LEFT JOIN gis_core.%I s ON s.centreline_id = intersection_latest.centreline_id_from
     	LEFT JOIN gis_core.%I e ON e.centreline_id = intersection_latest.centreline_id_to
  		WHERE intersection_latest.connected = ''N''::text AND s.id IS NOT NULL AND e.id IS NOT NULL;',
        partition_table, ver, centreline_table, routing_table, routing_table
    );

    -- Add check constraint (constraint name = ver)
    EXECUTE format(
        'ALTER TABLE gis_core.%I
         ADD CONSTRAINT %I CHECK (version_date = %L);',
        partition_table, ver, ver
    );

    -- Attach partition
    EXECUTE format(
        'ALTER TABLE gis_core.centreline_routing_restrictions_higher_rc
         ATTACH PARTITION gis_core.%I
         FOR VALUES IN (%L);',
        partition_table, ver
    );
END;
$$;

ALTER FUNCTION gis_core.create_routing_restriction_partition(text)
    OWNER TO gis_admins;