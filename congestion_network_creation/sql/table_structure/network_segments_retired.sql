-- Table structure for network_segments_retired
-- which contains all retired segments 

CREATE TABLE congestion.network_segments_retired (
    segment_id bigint,
    start_vid bigint,
    end_vid bigint,
    geom geometry,
    total_length numeric,
    highway boolean,
    start_int bigint,
    end_int bigint,
    start_px bigint,
    end_px bigint,
    here_version text,
    centreline_version text,
    retired_date date,
    retired_reason text,
    replaced_id int [],
    valid_from date,
    valid_to date
);


COMMENT ON TABLE congestion.network_segments_retired IS
'Containing all retired segments in the congestion network.';


INSERT INTO congestion.network_segments_retired
SELECT
    segment_id,
    start_vid,
    end_vid,
    geom,
    total_length,
    highway,
    f.int_id AS start_int,
    t.int_id AS end_int,
    f.px AS start_px,
    t.px AS end_px,
    '21_1' AS here_version,
    NULL AS centreline_version,
    '2022-07-20' AS retired_date,
    'outdated' AS retired_reason,
    NULL AS replaced_id,
    NULL AS valid_from,
    NULL AS valid_to

FROM congestion.network_segments
LEFT JOIN congestion.network_int_px_21_1_updated AS f ON start_vid = node_id
LEFT JOIN congestion.network_int_px_21_1_updated AS t ON end_vid = t.node_id
WHERE segment_id IN (3588, 3589)
