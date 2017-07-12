CREATE OR REPLACE FUNCTION dist_frac(link1 geometry, link2 geometry) RETURNS double precision
	AS
$BODY$
DECLARE
	len double precision;
	dist1 double precision;
	dist2 double precision;
	frac double precision;
BEGIN
	len := ST_Distance(ST_StartPoint(ST_Transform(ST_GeometryN(link1,1),32190)),ST_EndPoint(ST_Transform(ST_GeometryN(link1,ST_NumGeometries(link1)),32190)));

	dist1 := ST_Distance(ST_ClosestPoint(ST_Transform(link2,32190),ST_EndPoint(ST_Transform(ST_GeometryN(link1,1),32190))),
		ST_EndPoint(ST_Transform(ST_GeometryN(link1,1),32190)));
		
	dist2 := ST_Distance(ST_ClosestPoint(ST_Transform(link2,32190),ST_StartPoint(ST_Transform(ST_GeometryN(link1,1),32190))),
		ST_StartPoint(ST_Transform(ST_GeometryN(link1,1),32190)));

	frac := GREATEST(dist2,dist1)/len;
	RETURN frac;
END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;