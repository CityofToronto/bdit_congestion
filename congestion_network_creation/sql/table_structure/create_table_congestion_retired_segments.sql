-- Table: congestion.congestion_retired_segments

-- DROP TABLE IF EXISTS congestion.congestion_retired_segments;

CREATE TABLE IF NOT EXISTS congestion.congestion_retired_segments
(
    old_segment_id bigint,
    new_segment_ids bigint[],
    old_ver text,
    new_ver text
);

ALTER TABLE IF EXISTS congestion.congestion_retired_segments
    OWNER to congestion_admins;

REVOKE ALL ON TABLE congestion.congestion_retired_segments FROM bdit_humans;

GRANT SELECT ON TABLE congestion.congestion_retired_segments TO bdit_humans;

GRANT ALL ON TABLE congestion.congestion_retired_segments TO congestion_admins;