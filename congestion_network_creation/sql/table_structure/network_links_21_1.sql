CREATE TABLE IF NOT EXISTS congestion.network_links_21_1
(
    segment_id bigint,
    start_vid bigint,
    end_vid bigint,
    link_dir text,
    geom geometry,
    length numeric
);

ALTER TABLE congestion.network_links_21_1
    OWNER to congestion_admins;

COMMENT ON TABLE congestion.network_links_21_1
    IS 'A lookup table for the congestion network, contains segment_id and link_dir (21_1 map version) lookup.';