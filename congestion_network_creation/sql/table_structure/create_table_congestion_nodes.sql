CREATE TABLE IF NOT EXISTS congestion.congestion_nodes
(
    node_id bigint,
    geom geometry,
    ver_id text,
    CONSTRAINT unique_node_id_ver_id
        UNIQUE (node_id, ver_id)
) PARTITION BY LIST (ver_id);

ALTER TABLE IF EXISTS congestion.congestion_nodes
    OWNER TO congestion_admins;

GRANT SELECT ON TABLE congestion.congestion_nodes TO bdit_humans;
GRANT ALL ON TABLE congestion.congestion_nodes TO congestion_admins;

COMMENT ON TABLE congestion.congestion_nodes
    IS 'Partition table that contains all versions of congestion nodes.';

CREATE INDEX ON congestion.congestion_nodes (ver_id);
