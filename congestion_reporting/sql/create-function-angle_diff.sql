CREATE OR REPLACE FUNCTION angle_diff(link1 geometry, link2 geometry) RETURNS double precision
	AS
$BODY$
DECLARE
	angle double precision;
	angle1 double precision;
	angle2 double precision;
BEGIN
	angle1 := degrees(ST_Azimuth(ST_StartPoint(ST_Transform(ST_GeometryN(link1,1),32190)),ST_EndPoint(ST_Transform(ST_GeometryN(link1,ST_NumGeometries(link1)),32190))));
	angle2 := degrees(ST_Azimuth(ST_ClosestPoint(ST_Transform(link2,32190),ST_StartPoint(ST_Transform(ST_GeometryN(link1,1),32190))),
		ST_ClosestPoint(ST_Transform(link2,32190),ST_EndPoint(ST_Transform(ST_GeometryN(link1,ST_NumGeometries(link1)),32190)))
		));

	IF (angle1 > 180) THEN
		angle1 := angle1 - 180;
	END IF;

	IF (angle2 > 180) THEN
		angle2 := angle2 - 180;
	END IF;

	angle := abs(angle1 - angle2);
	RETURN angle;
END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;