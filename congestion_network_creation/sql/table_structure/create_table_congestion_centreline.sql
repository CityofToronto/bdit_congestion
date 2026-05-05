CREATE TABLE IF NOT EXISTS congestion.congestion_centreline
(
    segment_id bigint,
    streetname text,
    from_int integer,
    from_int_desc text,
	to_int integer,
    to_int_desc text,
	centreline_ids int[],
    centreline_uids text[],
	geom geometry ,
    ver_id text,
    CONSTRAINT unique_segment_id_from_int_to_int_ver_id
        UNIQUE (segment_id, from_int, to_int, ver_id)
) PARTITION BY LIST (ver_id);

ALTER TABLE IF EXISTS congestion.congestion_centreline
    OWNER TO congestion_admins;

GRANT SELECT ON TABLE congestion.congestion_centreline TO bdit_humans;
GRANT ALL ON TABLE congestion.congestion_centreline TO congestion_admins;

COMMENT ON TABLE congestion.congestion_centreline
    IS 'Partition table that contains all versions of congestion centreline lookup table.';

CREATE INDEX ON congestion.congestion_centreline (ver_id);