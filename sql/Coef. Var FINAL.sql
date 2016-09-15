--insert computed data into empty table created in the previous query . This table is specifially to aggregate data across the columns for the table congestion ranking 
INSERT INTO covar_to_gis

SELECT*FROM(
SELECT*FROM
(	SELECT
		extract_hour_14.tmc,
		
		Inrix_Steeles.Miles*1.60934 AS Length,
		
		Inrix_Steeles.RoadNumber, 
		
		Inrix_Steeles.RoadName, 
		
		TMC_StreetLookup.Dir_Clean,
		
		TMC_StreetLookup.Street_Clean,
		
		TMC_StreetLookup.FromTo_Clean,
		
		16::integer AS "Hour of Analysis" --input hour of analysis. Change everytime new hour is queried 
		
	FROM (TMC_StreetLookup INNER JOIN extract_hour_14 ON TMC_StreetLookup.tmc = extract_hour_14.tmc) INNER JOIN Inrix_Steeles ON extract_hour_14.tmc = Inrix_Steeles.tmc

	GROUP BY extract_hour_14.tmc, length, RoadNumber, RoadName, Dir_Clean, Street_Clean, FromTo_Clean 
	
	ORDER BY extract_hour_14.tmc
	
)AS part1 --This section joins with the rest of result set including: Coif-Var and Rank Monthly, Coef-Var and Rank Quarterly, Coif-Var and Rank Annual 

JOIN 
(
	------------------------ Monthly Coef. Var. and Rank (2 parts apphended) --------------------------------
	SELECT*FROM
	(
		SELECT*FROM 
		crosstab--cross tab takes three columns and pivots the row of data for one column into seperate unique columns (unique month rows are now unique columns) 
		(
			$$SELECT  --selects three columns: tmc, month and TTI 
				extract_hour_14.tmc, 
				
				extract_hour_14.month, 

				--calculates the coefficient variance of the speeds over each month 
				stddev_samp(extract_hour_14.speed)/avg(extract_hour_14.speed) 
				 
			FROM (INRI_speed85 INNER JOIN extract_hour_14 ON INRI_speed85.tmc = extract_hour_14.tmc) 

			--INSERT DAY AND HOUR -- 
			WHERE (inrix_speed85.Type)<'3' AND (day_month_allyears.weekday)>0 And (day_month_allyears.weekday)<7 AND trunc(day_month_allyears.time15/10) = 16  AND (inrix_speed85.hwy_or_art)='2'


			GROUP BY month, extract_hour_14.tmc, INRI_speed85.speed
			
			ORDER BY 1$$,
			
			$$ SELECT month FROM generate_series(1,12) month $$--the coef. var. data are now spread across each month of the year 
		 )AS (tmc1 int, "Jan TTI" double precision, "Feb Coef-Var" double precision, "Mar Coef-Var" double precision, "Apr Coef-Var" double precision, "May Coef-Var" double precision, "Jun Coef-Var" double precision, "Jul Coef-Var" double precision, "Aug Coef-Var" double precision, "Sep Coef-Var" double precision, "Oct Coef-Var" double precision, "Nov Coef-Var" double precision, "Dec Coef-Var" double precision)

		
	)AS part2 --Subsection 1: Monthly Coef-Var  

	JOIN --- Joining two parts of monthly analysis 
	(	
		SELECT*FROM 
		crosstab
		(
			$$SELECT 
				extract_hour_14.tmc, 
				
				extract_hour_14.month, 
				
				RANK() OVER (partition by extract_hour_14.month ORDER BY stddev_samp(extract_hour_14.speed)/avg(extract_hour_14.speed) DESC)
				 
			FROM (INRI_speed85 INNER JOIN extract_hour_14 ON INRI_speed85.tmc = extract_hour_14.tmc)

			--INSERT DAY AND HOUR -- 
			WHERE (inrix_speed85.Type)<'3' AND (day_month_allyears.weekday)>0 And (day_month_allyears.weekday)<7 AND trunc(day_month_allyears.time15/10) = 16  AND (inrix_speed85.hwy_or_art)='2'


			GROUP BY month, extract_hour_14.tmc, INRI_speed85.speed
			
			ORDER BY 1$$,
			
			$$ SELECT month FROM generate_series(1,12) month $$
			
		)AS (tmc2 int, "Jan Rank" double precision, "Feb Rank" double precision, "Mar Rank" double precision, "Apr Rank" double precision, "May RANK" double precision, "Jun RANK" double precision, "Jul RANK" double precision, "Aug RANK" double precision, "Sep RANK" double precision, "Oct RANK" double precision, "Nov RANK" double precision, "Dec RANK" double precision) 
		

	)AS foo --Subsection 2: Monthly Coef-Var Rank 
	
	ON part2.tmc1 = foo.tmc2

	JOIN -- Joining monthly table to quarterly table 
	(	

		------------------------ Quarterly Coef-Var and Rank (2 parts apphended) --------------------------------
		SELECT*FROM
		(
			SELECT*FROM 
			crosstab
			(
				$$SELECT 
					extract_hour_14.tmc, 
			
					trunc(extract_hour_14.month/3)+1 AS quarter, 

					stddev_samp(extract_hour_14.speed)/avg(extract_hour_14.speed) 
			 
				FROM (INRI_speed85 INNER JOIN extract_hour_14 ON INRI_speed85.tmc = extract_hour_14.tmc) 

				--INSERT DAY AND HOUR -- 
				WHERE (inrix_speed85.Type)<'3' AND (day_month_allyears.weekday)>0 And (day_month_allyears.weekday)<7 AND trunc(day_month_allyears.time15/10) = 16  AND (inrix_speed85.hwy_or_art)='2'

				GROUP BY quarter, extract_hour_14.tmc, INRI_speed85.speed
		
				ORDER BY 1$$,
				
				$$ SELECT month FROM generate_series(1,4) month $$
				
			)AS (tmc3 int, "Quarter1 Coef-Var" double precision, "Quarter2 Coef-Var" double precision, "Quarter3 Coef-Var" double precision, "Quarter4 Coef-Var" double precision) 
			
		)AS part3 -- Subsection 1 : Quarterly Coef-Var

		JOIN ---Joining two parts of quarterly analysis 
		(
			SELECT*FROM 
			crosstab
			(
				$$SELECT 
					extract_hour_14.tmc, 
			
					trunc(extract_hour_14.month/3)+1 AS quarter, 

					RANK() OVER (partition by trunc(extract_hour_14.month/3)+1 ORDER BY stddev_samp(extract_hour_14.speed)/avg(extract_hour_14.speed) DESC)
			 
				FROM (INRI_speed85 INNER JOIN extract_hour_14 ON INRI_speed85.tmc = extract_hour_14.tmc) 

				--INSERT DAY AND HOUR -- 
				WHERE (inrix_speed85.Type)<'3' AND (day_month_allyears.weekday)>0 And (day_month_allyears.weekday)<7 AND trunc(day_month_allyears.time15/10) = 16  AND (inrix_speed85.hwy_or_art)='2'


				GROUP BY quarter, extract_hour_14.tmc, INRI_speed85.speed
		
				ORDER BY 1$$,
				
				$$ SELECT month FROM generate_series(1,4) month $$
				
			)AS (tmc4 int, "Quarter1 RANK" double precision, "Quarter2 RANK" double precision, "Quarter3 RANK" double precision, "Quarter4 RANK" double precision)
			 
		)AS foo2 --Subsection 2 - Quarterly Coef-Var Rank 

		ON part3.tmc3 = foo2.tmc4
		
	)AS foo3 

	ON foo.tmc2 = foo3.tmc4
	
	JOIN -- Joining annual analysis to monthly+quarterly analysis 
	(
		------------------------ Annual Coef-Var and Rank (apphends 2 parts) --------------------------------
		SELECT*FROM
		(

			SELECT 

				extract_hour_14.tmc AS tmc5, 
					
				stddev_samp(extract_hour_14.speed)/avg(extract_hour_14.speed) AS "ANNUAL Coef-Var" 
					
			FROM (INRI_speed85 INNER JOIN extract_hour_14 ON INRI_speed85.tmc = extract_hour_14.tmc) INNER JOIN Inrix_Steeles ON extract_hour_14.tmc = Inrix_Steeles.tmc

			--INSERT DAY AND HOUR -- 
			WHERE (inrix_speed85.Type)<'3' AND (day_month_allyears.weekday)>0 And (day_month_allyears.weekday)<7 AND trunc(day_month_allyears.time15/10) = 16  AND (inrix_speed85.hwy_or_art)='2'


			GROUP BY extract_hour_14.tmc, INRI_speed85.speed
	
			
		)AS part4 ---Subsection 1: Annual TTI 

			JOIN --- Joining two parts of annual analysis 
			(

				SELECT 

					extract_hour_14.tmc AS tmc6, 
					
					RANK() OVER (ORDER BY stddev_samp(extract_hour_14.speed)/avg(extract_hour_14.speed) DESC) AS "Annual RANK"
				 
				FROM (INRI_speed85 INNER JOIN extract_hour_14 ON INRI_speed85.tmc = extract_hour_14.tmc) INNER JOIN Inrix_Steeles ON extract_hour_14.tmc = Inrix_Steeles.tmc

				--INSERT DAY AND HOUR -- 
				WHERE (inrix_speed85.Type)<'3' AND (day_month_allyears.weekday)>0 And (day_month_allyears.weekday)<7 AND trunc(day_month_allyears.time15/10) = 16  AND (inrix_speed85.hwy_or_art)='2'
			

				GROUP BY extract_hour_14.tmc, INRI_speed85.speed
	

			)AS foo6 ---Subsection 1: Annual Coef Rank 

			ON part4.tmc5 = foo6.tmc6

	)AS foo4

	ON foo4.tmc6 = foo3.tmc4
	
)AS lastpart --- Alias to all subtables joined after first section. 

ON lastpart.tmc6 = part1.tmc
)as finaltable

ORDER BY  "Annual RANK" ;


ALTER TABLE covar_to_gis
DROP COLUMN tmc1, DROP COLUMN tmc2, DROP COLUMN tmc3, DROP COLUMN tmc4, DROP COLUMN tmc5, DROP COLUMN tmc6 ; 

