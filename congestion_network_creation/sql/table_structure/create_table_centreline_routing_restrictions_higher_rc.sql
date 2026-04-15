CREATE TABLE IF NOT EXISTS gis_core.centreline_routing_restrictions_higher_rc
(
    path bigint[],
    cost integer,
    version_date text
) PARTITION BY LIST (version_date);

ALTER TABLE IF EXISTS gis_core.centreline_routing_restrictions_higher_rc
    OWNER to gis_admins;

REVOKE ALL ON TABLE gis_core.centreline_routing_restrictions_higher_rc FROM bdit_humans;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE gis_core.centreline_routing_restrictions_higher_rc TO bdit_humans;

GRANT ALL ON TABLE gis_core.centreline_routing_restrictions_higher_rc TO gis_admins;

COMMENT ON TABLE gis_core.centreline_routing_restrictions_higher_rc
    IS '''A view that contains centreline streets for routing, with duplicated rows 
for two-way streets and flipped geometries when lines were drawn against 
digitization. A new id has been assigned to each centreline to distinguish 
duplicated lines.''';