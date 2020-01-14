CREATE TABLE congestion.segments (
	segment_id int NOT NULL,
	length_m numeric
);
COMMENT ON TABLE congestion.segments
  IS 'Lookup table for Grid-based segments';