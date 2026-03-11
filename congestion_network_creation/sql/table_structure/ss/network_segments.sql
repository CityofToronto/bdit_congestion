-- Table Structure
CREATE TABLE congestion.network_segments 
(
    segment_id bigint NOT NULL,
    start_vid bigint  NOT NULL,
    end_vid bigint  NOT NULL,
    geom geometry  NOT NULL,
    total_length numeric  NOT NULL,
    highway boolean  NOT NULL,
    direction text  
)
ALTER TABLE congestion.network_segments
    OWNER to congestion_admins;

COMMENT ON TABLE congestion.network_segments
    IS 'Contains the most up-to-date segments for the congestion network.';