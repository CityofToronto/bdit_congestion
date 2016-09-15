
--set to timezone of the raw data  
BEGIN; 
set timezone to 'UTC';


--create raw data table (empty) 
CREATE TABLE raw_data_allyears 
(
	dateandtime timestamp without time zone,
	tmc character varying,
	speed integer
)

WITH (
  OIDS=FALSE
);
ALTER TABLE raw_data_allyears
  OWNER TO pgis;

--fill data table with data from the McMaster data files  
--change file paths accordingly (can copy all mcmaster files, 3-4 copy systems at a time will prevent system overload) 
COPY raw_data_allyears FROM 'C:\Big Data Group\Data\original_data\McMaster_Toronto_201401To201403.csv' DELIMITER ',' CSV;

COPY raw_data_allyears FROM 'C:\Big Data Group\Data\original_data\McMaster_Toronto_201404To201406.csv' DELIMITER ',' CSV;

COPY raw_data_allyears FROM 'C:\Big Data Group\Data\original_data\McMaster_Toronto_201407To201409.csv' DELIMITER ',' CSV;

COPY raw_data_allyears FROM 'C:\Big Data Group\Data\original_data\McMaster_Toronto_201410To201412.csv' DELIMITER ',' CSV;

END; 

--adjust data types from default types to correct types
ALTER TABLE raw_data_allyears  ALTER COLUMN dateandtime TYPE timestamp with time zone USING dateandtime:: timestamp with time zone;

--change the timezone to current timezone. Dates will convert to this timezone 
Set timezone = 'America/Toronto';