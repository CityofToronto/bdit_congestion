CREATE OR REPLACE FUNCTION pretty_quarter(quarter DATE) 
RETURNS char(7) 
AS $$
BEGIN 
	RETURN to_char(quarter,'YYYY')||' '||'Q'||date_part('quarter',quarter);
END;
$$LANGUAGE plpgsql
