CREATE TABLE IF NOT EXISTS congestion.network_baseline
(
    segment_id bigint,
    baseline_10pct double precision,
    baseline_15pct double precision,
    baseline_20pct double precision,
    baseline_25pct double precision
);

ALTER TABLE congestion.network_baseline
OWNER TO congestion_admins;
