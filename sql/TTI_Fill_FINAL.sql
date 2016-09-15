
--insert computed data into empty table created in the previous query . This table is specifially to aggregate data across the columns for the table congestion ranking 
INSERT INTO tti_to_gis

SELECT*FROM(

SELECT*FROM
(	SELECT
		day_month_allyears.tmc AS tmc,
		
		to_inrix_steeles.Miles*1.60934 AS Length,
		
		to_inrix_steeles.RoadNumber AS RoadNumber, 
		
		to_inrix_steeles.RoadName AS RoadName, 
		
		tmc_street_lookup.Dir_Clean AS Dir_Clean,
		
		tmc_street_lookup.Street_Clean AS Street_Clean,
		
		tmc_street_lookup.FromTo_Clean AS FromTo_Clean, 

		16::integer AS "Hour of Analysis" --input hour of analysis. Change everytime new hour is queried 
		
	FROM (tmc_street_lookup RIGHT JOIN day_month_allyears ON tmc_street_lookup.tmc = day_month_allyears.tmc) LEFT JOIN to_inrix_steeles ON day_month_allyears.tmc = to_inrix_steeles.tmc

	GROUP BY day_month_allyears.tmc, Length, RoadNumber, RoadName, Dir_Clean, Street_Clean, FromTo_Clean 
	
	
	
)AS part1 --This section joins with the rest of result set including: TTI and Rank Monthly, TTI and Rank Quarterly, TTI and Rank Annual 



INNER JOIN 
(
	SELECT*FROM
	(
		SELECT*FROM 
		crosstab --cross tab takes three columns and pivots the row of data for one column into seperate unique columns (unique month rows are now unique columns) 
		(

			$$SELECT --selects three columns: tmc, month and TTI 
				day_month_allyears.tmc, 
				
				day_month_allyears.month, 

				--calculates the the TTI 
				inrix_speed85.speed/avg(day_month_allyears.speed_wtd) AS TTI  
				 
			FROM (inrix_speed85 INNER JOIN day_month_allyears ON inrix_speed85.tmc = day_month_allyears.tmc) 

			--INSERT DAY AND HOUR -- 
			WHERE (inrix_speed85.Type)<'3' AND (day_month_allyears.weekday)>0 And (day_month_allyears.weekday)<7 AND trunc(day_month_allyears.time15/10) = 16  AND (inrix_speed85.hwy_or_art)='2'

			GROUP BY month,  day_month_allyears.tmc, inrix_speed85.speed
			
			ORDER BY 1$$,
			
			$$ SELECT month FROM generate_series(1,12) month $$ --the tti data are now spread across each month of the year 
		 )AS (tmc1 character varying, "Jan TTI" double precision, "Feb TTI" double precision, "Mar TTI" double precision, "Apr TTI" double precision, "May TTI" double precision, "Jun TTI" double precision, "Jul TTI" double precision, "Aug TTI" double precision, "Sep TTI" double precision, "Oct TTI" double precision, "Nov TTI" double precision, "Dec TTI" double precision)

		
	)AS part2 --Subsection 1: Monthly TTI  

	INNER JOIN --- Joining two parts of monthly analysis 
	(	
		SELECT*FROM 
		crosstab
		(
			$$SELECT 
				day_month_allyears.tmc, 
				
				day_month_allyears.month, 
				
				RANK() OVER (partition by day_month_allyears.month ORDER BY inrix_speed85.speed/avg(day_month_allyears.speed_wtd) DESC) AS rank
				 
			FROM (inrix_speed85 INNER JOIN day_month_allyears ON inrix_speed85.tmc = day_month_allyears.tmc)

			--INSERT DAY AND HOUR -- 
			WHERE (inrix_speed85.Type)<'3' AND (day_month_allyears.weekday)>0 And (day_month_allyears.weekday)<7 AND trunc(day_month_allyears.time15/10) = 16  AND (inrix_speed85.hwy_or_art)='2'

			GROUP BY month, day_month_allyears.tmc, inrix_speed85.speed
			
			ORDER BY 1$$,
			
			$$ SELECT month FROM generate_series(1,12) month $$
			
		)AS (tmc2 character varying, "Jan TT1 Rank" double precision, "Feb TTI Rank" double precision, "Mar TTI Rank" double precision, "Apr TTI Rank" double precision, "May TTI RANK" double precision, "Jun TTI RANK" double precision, "Jul TTI RANK" double precision, "Aug TTI RANK" double precision, "Sep TTI RANK" double precision, "Oct TTI RANK" double precision, "Nov  TTI RANK" double precision, "Dec TTI RANK" double precision) 
		

	)AS foo --Subsection 2: Monthly TTI Rank 
	
	ON part2.tmc1 = foo.tmc2

	INNER JOIN -- Joining monthly table to quarterly table 
	(	

		------------------------ Quarterly TTI and Rank (2 parts apphended) --------------------------------
		SELECT*FROM
		(
			SELECT*FROM 
			crosstab
			(
				$$SELECT 
					day_month_allyears.tmc, 
			
					trunc(day_month_allyears.month/3)+1 AS quarter, 

					inrix_speed85.speed/avg(day_month_allyears.speed_wtd) AS TTI  
			 
				FROM (inrix_speed85 INNER JOIN day_month_allyears ON inrix_speed85.tmc = day_month_allyears.tmc) 

				--INSERT DAY AND HOUR -- 
				WHERE (inrix_speed85.Type)<'3' AND (day_month_allyears.weekday)>0 And (day_month_allyears.weekday)<7 AND trunc(day_month_allyears.time15/10) = 16  AND (inrix_speed85.hwy_or_art)='2'
				GROUP BY quarter, day_month_allyears.tmc, inrix_speed85.speed
		
				ORDER BY 1$$,
				
				$$ SELECT month FROM generate_series(1,4) month $$
				
			)AS (tmc3 character varying, "Quarter1 TTI" double precision, "Quarter2 TTI" double precision, "Quarter3 TTI" double precision, "Quarter4 TTI" double precision) 
			
		)AS part3 -- Subsection 1 : Quaretrly TTI

		INNER JOIN ---Joining two parts of quarterly analysis 
		(
			SELECT*FROM 
			crosstab
			(
				$$SELECT 
					day_month_allyears.tmc, 
			
					trunc(day_month_allyears.month/3)+1 AS quarter, 

					RANK() OVER (partition by trunc(day_month_allyears.month/3)+1 ORDER BY inrix_speed85.speed/avg(day_month_allyears.speed_wtd) DESC) AS rank
			 
				FROM (inrix_speed85 INNER JOIN day_month_allyears ON inrix_speed85.tmc = day_month_allyears.tmc) 

				--INSERT DAY AND HOUR -- 
				WHERE (inrix_speed85.Type)<'3' AND (day_month_allyears.weekday)>0 And (day_month_allyears.weekday)<7 AND trunc(day_month_allyears.time15/10) = 16  AND (inrix_speed85.hwy_or_art)='2'

				GROUP BY quarter, day_month_allyears.tmc, inrix_speed85.speed
		
				ORDER BY 1$$,
				
				$$ SELECT month FROM generate_series(1,4) month $$
				
			)AS (tmc4 character varying, "Quarter1 TTI RANK" double precision, "Quarter2 TTI RANK" double precision, "Quarter3 TTI RANK" double precision, "Quarter4 TTI RANK" double precision)
			 
		)AS foo2 --Subsection 2 - Quarterly TTI Rank 

		ON part3.tmc3 = foo2.tmc4
		
	)AS foo3 

	ON foo.tmc2 = foo3.tmc4
	
	INNER JOIN -- Joining annual analysis to monthly+quarterly analysis 
	(
		------------------------ Annual TTI and Rank (apphends 2 parts) --------------------------------
		SELECT*FROM
		(

			SELECT 

				day_month_allyears.tmc AS tmc5, 
					
				inrix_speed85.speed/avg(day_month_allyears.speed_wtd) AS ANNUALTTI
					
			FROM (inrix_speed85 INNER JOIN day_month_allyears ON inrix_speed85.tmc = day_month_allyears.tmc) INNER JOIN to_inrix_steeles ON day_month_allyears.tmc = to_inrix_steeles.tmc

			--INSERT DAY AND HOUR -- 
			WHERE (inrix_speed85.Type)<'3' AND (day_month_allyears.weekday)>0 And (day_month_allyears.weekday)<7 AND trunc(day_month_allyears.time15/10) = 16  AND (inrix_speed85.hwy_or_art)='2'

			GROUP BY  day_month_allyears.tmc, inrix_speed85.speed
	
			
		)AS part4 ---Subsetion 1: Annual TTI 

			INNER JOIN --- Joining two parts of annual analysis 
			(

				SELECT 

					day_month_allyears.tmc AS tmc6, 
					
					RANK() OVER (ORDER BY inrix_speed85.speed/avg(day_month_allyears.speed_wtd) DESC) AS "Annual TTI RANK"
				 
				FROM (inrix_speed85 INNER JOIN day_month_allyears ON inrix_speed85.tmc = day_month_allyears.tmc) INNER JOIN to_inrix_steeles ON day_month_allyears.tmc = to_inrix_steeles.tmc

				--INSERT DAY AND HOUR -- 
				WHERE (inrix_speed85.Type)<'3' AND (day_month_allyears.weekday)>0 And (day_month_allyears.weekday)<7 AND trunc(day_month_allyears.time15/10) = 16  AND (inrix_speed85.hwy_or_art)='2'

				GROUP BY day_month_allyears.tmc, inrix_speed85.speed
	

			)AS foo6 ---Subsetion 1: Annual TTI Rank 

			ON part4.tmc5 = foo6.tmc6

	)AS foo4 

	ON foo4.tmc6 = foo3.tmc4
	
)AS lastpart --- Alias to all subtables following first section

ON lastpart.tmc6 = part1.tmc


--) 
)as finaltable

ORDER BY  "Annual TTI RANK" ;


ALTER TABLE tti_to_gis
DROP COLUMN tmc1, DROP COLUMN tmc2, DROP COLUMN tmc3, DROP COLUMN tmc4, DROP COLUMN tmc5, DROP COLUMN tmc6 ; 
