CREATE TABLE IF NOT EXISTS congestion.congestion_links
(
    segment_id bigint,
    start_vid bigint,
    end_vid bigint,
    link_dir text,
    length double precision,
    geom geometry,
    ver_id text,
    CONSTRAINT unique_segment_id_link_dir_ver_id
        UNIQUE (segment_id, link_dir, ver_id)
) PARTITION BY LIST (ver_id);

ALTER TABLE IF EXISTS congestion.congestion_links
    OWNER TO congestion_admins;

GRANT SELECT ON TABLE congestion.congestion_links TO bdit_humans;
GRANT ALL ON TABLE congestion.congestion_links TO congestion_admins;

COMMENT ON TABLE congestion.congestion_links
    IS 'Partition table that contains all versions of congestion segment links.';

CREATE INDEX ON congestion.congestion_links (ver_id);