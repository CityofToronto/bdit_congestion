CREATE TABLE IF NOT EXISTS gis_core.routing_centreline_directional_higher_rc
(
    centreline_id integer,
    id bigint,
    centreline_uid text,
    source integer,
    target integer,
    cost_length numeric,
    geom geometry,
	version_date text
) PARTITION BY LIST (version_date);
;

ALTER TABLE IF EXISTS gis_core.routing_centreline_directional_higher_rc
    OWNER to gis_admins;

REVOKE ALL ON TABLE gis_core.routing_centreline_directional_higher_rc FROM bdit_humans;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE gis_core.routing_centreline_directional_higher_rc TO bdit_humans;

GRANT ALL ON TABLE gis_core.routing_centreline_directional_higher_rc TO gis_admins;

COMMENT ON TABLE gis_core.routing_centreline_directional_higher_rc
    IS '''A partition table that contains centreline streets for routing, with duplicated rows 
for two-way streets and flipped geometries when lines were drawn against 
digitization. A new id has been assigned to each centreline to distinguish 
duplicated lines. Only includes Collectors and up + Pending. Used for congestion conflation.''';