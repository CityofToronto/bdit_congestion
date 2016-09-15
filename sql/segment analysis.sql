CREATE TABLE day_month_steeles6
(
	tmc character varying, 
	time15 double precision, 
	year double precision, 
	month double precision, 
	weekday double precision,  
	speed_wtd numeric,
	count_obs bigint, 
	avg_speed numeric 
);  



	INSERT INTO day_month_steeles6
	SELECT*FROM day_month_allyears
	WHERE tmc IN ('C09P08642',	'C09N08642',
'C09P07518',	'C09N07518'
) 
	ORDER BY tmc, time15, month, weekday;

	COPY day_month_steeles6 TO 'C:\Big Data Group\Data\original_data\temp1.csv' DELIMITER ',' CSV HEADER;

	