
--creating the intermediary tables, mock or real. 
--adjust table names according to which tables you want to make 


--creates empty table with column headings 
CREATE TABLE Inri_speed85 
(
tmc character varying,
speed text,
nightspeed text,
type text,
hwy_or_art text
); 

--copies data from csv into table. Make sure the order of columns are same for both the csv and empty table created above 
COPY Inri_speed85 FROM 'C:\Big Data Group\Data\original_data\Mock Data\speed85_mock.csv' DELIMITER ',' CSV; 

------------------------------------

CREATE TABLE tmc_streetlookup
(
tmc character varying, 
street_Clean text,
Dir_Clean text,
FromTo_Clean text
);

COPY tmc_streetlookup FROM 'C:\Big Data Group\Data\original_data\Mock Data\lookup_mock.csv' DELIMITER ',' CSV; 

------------------------------------ 
 
CREATE TABLE Inrix_Steeles
(
	tmc character varying,
	FIRST_Inri text,
	SUM_Miles text,
	Shape_len text,
	TMC_1 character varying,
	Type character varying,
	RoadNumber character varying,
	RoadName character varying,
	FirstName character varying,
	LinearTMC character varying, 
	Country text,
	State text,
	County text,
	ZIP text,
	Direction text, 
	StartLat text,
	StartLong text,
	EndLat text,
	EndLong text,
	Miles text,
	Tmc_12 character varying,
	Speed text,
	Sum_mile_1 text
);

COPY Inrix_Steeles FROM 'C:\Big Data Group\Data\original_data\Mock Data\steeles_mock.csv' DELIMITER ',' CSV;  
 
------------------Converting data types 

ALTER TABLE Inri_speed85  ALTER COLUMN speed TYPE double precision USING speed:: double precision;
ALTER TABLE Inri_speed85  ALTER COLUMN nightspeed TYPE double precision USING nightspeed:: double precision;

ALTER TABLE Inrix_Steeles ALTER COLUMN SUM_Miles TYPE double precision USING SUM_Miles:: double precision;
ALTER TABLE Inrix_Steeles ALTER COLUMN Shape_len TYPE double precision USING Shape_len:: double precision;
ALTER TABLE Inrix_Steeles ALTER COLUMN StartLat TYPE numeric USING StartLat:: numeric;
ALTER TABLE Inrix_Steeles ALTER COLUMN StartLong TYPE numeric USING StartLong:: numeric;
ALTER TABLE Inrix_Steeles ALTER COLUMN EndLat TYPE numeric USING EndLat:: numeric;
ALTER TABLE Inrix_Steeles ALTER COLUMN EndLong TYPE numeric USING EndLong:: numeric;
ALTER TABLE Inrix_Steeles ALTER COLUMN miles TYPE double precision USING miles:: double precision;
ALTER TABLE Inrix_Steeles ALTER COLUMN speed TYPE double precision USING speed:: double precision;
ALTER TABLE Inrix_Steeles ALTER COLUMN Sum_mile_1 TYPE double precision USING Sum_mile_1:: double precision;  

ALTER TABLE Inrix_Steeles ALTER COLUMN tmc TYPE integer USING tmc:: integer; 
ALTER TABLE Inri_speed85 ALTER COLUMN tmc TYPE integer USING tmc:: integer; 
ALTER TABLE TMC_streetlookup ALTER COLUMN tmc TYPE integer USING tmc:: integer; */ 