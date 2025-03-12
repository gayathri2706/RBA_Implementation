SET @order_counter := 0;

SET @prev_part_id := NULL;

SET @prev_shift := NULL;

SET @prev_date := NULL;

SELECT  

   DATE_FORMAT(ProductionDate, '%d-%m-%Y') AS ProductionDate,

   Shift,

   PatternIdentification,

   TIME(Start_Time1) AS StartTime,

   TIME(End_Time2) AS EndTime,

   TotalPourStatus

FROM (

   SELECT  

       MIN(AdjustedDate) AS ProductionDate,

       Shift,

       PatternIdentification,

       MIN(CombinedDateTime) AS Start_Time1,

       MAX(CombinedDateTime) AS End_Time2,

       SUM(PourStatus) AS TotalPourStatus

   FROM (

       SELECT  

           PatternIdentification,

           Shift,

           AdjustedDate,

           Date,

           Time,

           PourStatus,

           TimePour AS CombinedDateTime,

           @order_counter :=  

               CASE  

                   WHEN @prev_part_id = PatternIdentification

                    AND @prev_shift = Shift

                    AND @prev_date = AdjustedDate

                   THEN @order_counter  

                   ELSE (@order_counter := @order_counter + 1)

               END AS order_counter,

           @prev_part_id := PatternIdentification,

           @prev_shift := Shift,

           @prev_date := AdjustedDate

       FROM (

           SELECT  

               DATE(TimePour) AS `Date`,

               TIME(TimePour) AS `Time`,

               CASE  

                   WHEN TIME(TimePour) BETWEEN '07:00:00' AND '18:59:59' THEN 'A'

                   WHEN TIME(TimePour) BETWEEN '19:00:00' AND '23:59:59'

                     OR TIME(TimePour) BETWEEN '00:00:00' AND '06:59:59' THEN 'B'

                   ELSE NULL

               END AS `Shift`,

               CASE  

                   WHEN TIME(TimePour) < '07:00:00' THEN DATE_SUB(DATE(TimePour), INTERVAL 1 DAY)

                   ELSE DATE(TimePour)

               END AS AdjustedDate,

               PatternIdentification,

               PourStatus,

               TimePour

           FROM rba_data.moulding_machine_data

           WHERE TimePour >= '2025-03-01 07:00:00'

       ) AS Table2

       ORDER BY Date, Time

   ) AS numbered

   GROUP BY order_counter, PatternIdentification, Shift

   ORDER BY ProductionDate, Start_Time1

) AS table3

ORDER BY ProductionDate, Start_Time1;
 
