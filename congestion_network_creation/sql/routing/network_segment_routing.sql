CREATE MATERIALIZED VIEW congestion.network_segment_routing AS

SELECT
    segment_id,
    start_vid::integer AS source,
    end_vid::integer AS target,
    total_length::integer AS cost,
    geom

FROM congestion.network_segments;

ALTER TABLE congestion.network_segment_routing
OWNER TO congestion_admins;

COMMENT ON MATERIALIZED VIEW congestion.network_segment_routing
IS 'Routing network for network segments. Contains source, target and cost for each segment in the congestion network.';

GRANT SELECT ON TABLE congestion.network_segment_routing TO bdit_humans;