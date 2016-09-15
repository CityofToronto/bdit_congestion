--Making an empty table template for the final congesting reporting table. Chnage column and table headings for different reports (coef. var., buffer time, congesting ranking 2015) 
DROP TABLE IF EXISTS inrix.tti_to_gis; --adjust 

CREATE TABLE inrix.tti_to_gis --adjust title for each report 
(
  tmc character varying,
  length double precision,
  roadnumber character varying,
  roadname character varying,
  dir_clean text,
  street_clean text,
  fromto_clean text,
  hh integer, --hour of analysis
  tmc1 char(9),
  Jan_tti double precision, --for other metrics, change title (e.g. Jan Coef Var) 
  Feb_tti double precision,
  Mar_tti double precision,
  Apr_tti double precision,
  May_tti double precision,
  Jun_tti double precision,
  Jul_tti double precision,
  Aug_tti double precision,
  Sep_tti double precision,
  Oct_tti double precision,
  Nov_tti double precision,
  Dec_tti double precision,
  tmc2 char(9),
  Jan_tti_rank double precision, --for other metrics, change title (e.g. Jan Coef Var Rank) 
  Feb_tti_rank double precision,
  Mar_tti_rank double precision,
  Apr_tti_rank double precision,
  May_tti_rank double precision,
  Jun_tti_rank double precision,
  Jul_tti_rank double precision,
  Aug_tti_rank double precision,
  Sep_tti_rank double precision,
  Oct_tti_rank double precision,
  Nov_tti_rank double precision,
  Dec_tti_rank double precision,
  tmc3 char(9),
  Quarter1_tti double precision,
  Quarter2_tti double precision,
  Quarter3_tti double precision,
  Quarter4_tti double precision,
  tmc4 char(9),
  Quarter1_tti_rank double precision,
  Quarter2_tti_rank double precision,
  Quarter3_tti_rank double precision,
  Quarter4_tti_rank double precision,
  tmc5 char(9),
  annualtti double precision,
  tmc6 char(9),
  Annual_tti_rank bigint
)
WITH (
  OIDS=FALSE
);
