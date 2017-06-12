-- Function: line_dist(geometry, geometry)

-- DROP FUNCTION line_dist(geometry, geometry);

CREATE OR REPLACE FUNCTION line_dist(
    link_sub geometry,
    link_main geometry)
  RETURNS double precision AS
$BODY$
DECLARE
	start_dist double precision;
	end_dist double precision;
	result double precision;
BEGIN

	start_dist :=
		ST_Distance(
			ST_StartPoint(ST_Transform(ST_GeometryN(link_main,1),32190)),
			ST_ClosestPoint(ST_Transform(link_main,32190),ST_StartPoint(ST_Transform(ST_GeometryN(link_sub,1),32190)))
			)
			/
			ST_Length(ST_Transform(link_main,32190));
	end_dist :=
		ST_Distance(
			ST_StartPoint(ST_Transform(ST_GeometryN(link_main,1),32190)),
			ST_ClosestPoint(ST_Transform(link_main,32190),ST_EndPoint(ST_Transform(ST_GeometryN(link_sub,ST_NumGeometries(link_sub)),32190)))
			)
			/
			ST_Length(ST_Transform(link_main,32190));

	result := LEAST(start_dist, end_dist);

	RETURN result;
END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;
ALTER FUNCTION line_dist(geometry, geometry)
  OWNER TO aharpal;