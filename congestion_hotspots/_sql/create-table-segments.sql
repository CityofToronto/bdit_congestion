DROP TABLE congestion.segments;
CREATE TABLE congestion.segments (
	segment_id int NOT NULL,
	length_m numeric,
	geom geometry
);
COMMENT ON TABLE congestion.segments
  IS 'Lookup table for Grid-based segments';