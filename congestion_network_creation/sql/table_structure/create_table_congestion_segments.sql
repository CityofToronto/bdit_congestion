CREATE TABLE IF NOT EXISTS congestion.congestion_segments
(
    segment_id bigint,
    start_vid bigint,
    end_vid bigint,
    total_length double precision,
	dir text, 
    highway boolean,
	geom geometry,
    ver_id text,
    CONSTRAINT unique_segment_id_ver_id UNIQUE (segment_id, ver_id)
) PARTITION BY LIST (ver_id);


ALTER TABLE IF EXISTS congestion.congestion_segments
    OWNER to congestion_admins;
GRANT SELECT ON TABLE congestion.congestion_segments TO bdit_humans;

GRANT ALL ON TABLE congestion.congestion_segments TO congestion_admins;

COMMENT ON TABLE congestion.congestion_segments
    IS 'Partition Table that contains all versions of congestion network segments.';

CREATE INDEX ON congestion.congestion_segments (ver_id);