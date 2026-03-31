CREATE TABLE IF NOT EXISTS congestion.congestion_nodes_lookup
(
    node_id bigint,
    intersection_id integer,
	px text,
	intersection_desc text,
	highest_order_feature text ,
	node_geom geometry(Point,4326),
    int_geom geometry,
    ver_id text,
    CONSTRAINT unique_node_id_intersection_id_ver_id
        UNIQUE (node_id, intersection_id, ver_id)
) PARTITION BY LIST (ver_id);

ALTER TABLE IF EXISTS congestion.congestion_nodes_lookup
    OWNER TO congestion_admins;

GRANT SELECT ON TABLE congestion.congestion_nodes_lookup TO bdit_humans;
GRANT ALL ON TABLE congestion.congestion_nodes_lookup TO congestion_admins;

COMMENT ON TABLE congestion.congestion_nodes_lookup
    IS 'Partition table that contains all versions of congestion nodes lookup table (PX - traffic signal, and centreline intersection_id).';

CREATE INDEX ON congestion.congestion_nodes_lookup (ver_id);