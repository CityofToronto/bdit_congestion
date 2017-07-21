DROP TABLE IF EXISTS here_analysis.corridor_links_15min;

CREATE TABLE here_analysis.corridor_links_15min (
	corridor_id integer,
	link_dir text,
	datetime_bin timestamp without time zone,
	seq integer,
	spd_avg numeric,
	tt_avg numeric,
	obs smallint,
	excluded boolean,
	estimated boolean);


CREATE TABLE here_analysis.corridor_links_15min_201601 (
    CHECK ( datetime_bin >= DATE '2016-01-01' AND datetime_bin < DATE '2016-02-01' )
) INHERITS (here_analysis.corridor_links_15min);

CREATE TABLE here_analysis.corridor_links_15min_201602 (
    CHECK ( datetime_bin >= DATE '2016-02-01' AND datetime_bin < DATE '2016-03-01' )
) INHERITS (here_analysis.corridor_links_15min);

CREATE TABLE here_analysis.corridor_links_15min_201603 (
    CHECK ( datetime_bin >= DATE '2016-03-01' AND datetime_bin < DATE '2016-04-01' )
) INHERITS (here_analysis.corridor_links_15min);

CREATE TABLE here_analysis.corridor_links_15min_201604 (
    CHECK ( datetime_bin >= DATE '2016-04-01' AND datetime_bin < DATE '2016-05-01' )
) INHERITS (here_analysis.corridor_links_15min);

CREATE TABLE here_analysis.corridor_links_15min_201605 (
    CHECK ( datetime_bin >= DATE '2016-05-01' AND datetime_bin < DATE '2016-06-01' )
) INHERITS (here_analysis.corridor_links_15min);

CREATE TABLE here_analysis.corridor_links_15min_201606 (
    CHECK ( datetime_bin >= DATE '2016-06-01' AND datetime_bin < DATE '2016-07-01' )
) INHERITS (here_analysis.corridor_links_15min);

CREATE TABLE here_analysis.corridor_links_15min_201607 (
    CHECK ( datetime_bin >= DATE '2016-07-01' AND datetime_bin < DATE '2016-08-01' )
) INHERITS (here_analysis.corridor_links_15min);

CREATE TABLE here_analysis.corridor_links_15min_201608 (
    CHECK ( datetime_bin >= DATE '2016-08-01' AND datetime_bin < DATE '2016-09-01' )
) INHERITS (here_analysis.corridor_links_15min);

CREATE TABLE here_analysis.corridor_links_15min_201609 (
    CHECK ( datetime_bin >= DATE '2016-09-01' AND datetime_bin < DATE '2016-10-01' )
) INHERITS (here_analysis.corridor_links_15min);

CREATE TABLE here_analysis.corridor_links_15min_201610 (
    CHECK ( datetime_bin >= DATE '2016-10-01' AND datetime_bin < DATE '2016-11-01' )
) INHERITS (here_analysis.corridor_links_15min);

CREATE TABLE here_analysis.corridor_links_15min_201611 (
    CHECK ( datetime_bin >= DATE '2016-11-01' AND datetime_bin < DATE '2016-12-01' )
) INHERITS (here_analysis.corridor_links_15min);

CREATE TABLE here_analysis.corridor_links_15min_201612 (
    CHECK ( datetime_bin >= DATE '2016-12-01' AND datetime_bin < DATE '2017-01-01' )
) INHERITS (here_analysis.corridor_links_15min);


CREATE OR REPLACE FUNCTION corridor_links_15min_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF ( NEW.datetime_bin >= DATE '2016-01-01' AND
         NEW.datetime_bin < DATE '2016-02-01' ) THEN
        INSERT INTO here_analysis.corridor_links_15min_201601 VALUES (NEW.*);
    ELSIF ( NEW.datetime_bin >= DATE '2016-02-01' AND
         NEW.datetime_bin < DATE '2016-03-01' ) THEN
        INSERT INTO here_analysis.corridor_links_15min_201602 VALUES (NEW.*);
    ELSIF ( NEW.datetime_bin >= DATE '2016-03-01' AND
         NEW.datetime_bin < DATE '2016-04-01' ) THEN
        INSERT INTO here_analysis.corridor_links_15min_201603 VALUES (NEW.*);
    ELSIF ( NEW.datetime_bin >= DATE '2016-04-01' AND
         NEW.datetime_bin < DATE '2016-05-01' ) THEN
        INSERT INTO here_analysis.corridor_links_15min_201604 VALUES (NEW.*);
    ELSIF ( NEW.datetime_bin >= DATE '2016-05-01' AND
         NEW.datetime_bin < DATE '2016-06-01' ) THEN
        INSERT INTO here_analysis.corridor_links_15min_201605 VALUES (NEW.*);
    ELSIF ( NEW.datetime_bin >= DATE '2016-06-01' AND
         NEW.datetime_bin < DATE '2016-07-01' ) THEN
        INSERT INTO here_analysis.corridor_links_15min_201606 VALUES (NEW.*);
    ELSIF ( NEW.datetime_bin >= DATE '2016-07-01' AND
         NEW.datetime_bin < DATE '2016-08-01' ) THEN
        INSERT INTO here_analysis.corridor_links_15min_201607 VALUES (NEW.*);
    ELSIF ( NEW.datetime_bin >= DATE '2016-08-01' AND
         NEW.datetime_bin < DATE '2016-09-01' ) THEN
        INSERT INTO here_analysis.corridor_links_15min_201608 VALUES (NEW.*);
    ELSIF ( NEW.datetime_bin >= DATE '2016-09-01' AND
         NEW.datetime_bin < DATE '2016-10-01' ) THEN
        INSERT INTO here_analysis.corridor_links_15min_201609 VALUES (NEW.*);
    ELSIF ( NEW.datetime_bin >= DATE '2016-10-01' AND
         NEW.datetime_bin < DATE '2016-11-01' ) THEN
        INSERT INTO here_analysis.corridor_links_15min_201610 VALUES (NEW.*);
    ELSIF ( NEW.datetime_bin >= DATE '2016-11-01' AND
         NEW.datetime_bin < DATE '2016-12-01' ) THEN
        INSERT INTO here_analysis.corridor_links_15min_201611 VALUES (NEW.*);
    ELSIF ( NEW.datetime_bin >= DATE '2016-12-01' AND
         NEW.datetime_bin < DATE '2017-01-01' ) THEN
        INSERT INTO here_analysis.corridor_links_15min_201612 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Date out of range.';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER insert_corridor_links_15min_trigger
    BEFORE INSERT ON here_analysis.corridor_links_15min
    FOR EACH ROW EXECUTE PROCEDURE corridor_links_15min_insert_trigger();