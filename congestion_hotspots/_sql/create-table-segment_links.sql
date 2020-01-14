DROP TABLE congestion.segment_links;
CREATE TABLE congestion.segment_links (
	segment_link_id serial NOT NULL,
    segment_id integer NOT NULL,
    link_dir text
);
COMMENT ON TABLE congestion.segment_links
  IS 'Intermediate table for linking HERE links to grid-based segments';