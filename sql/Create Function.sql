CREATE FUNCTION getbuffertime2(x double precision) RETURNS void AS $$ 
	SELECT 
		(avg(x)*((1/(percentile_cont(0.95) WITHIN GROUP (ORDER BY x))) - (1/avg(x)))) *(-100);
 

	$$ LANGUAGE SQL;


	--SELECT getbuffer(); 