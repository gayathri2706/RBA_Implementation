SET @BatchGroup = 0;
SET @PrevPattern = NULL;

 

SELECT 
    ProductionDate,
    Shift,
    PatternIdentification,
    MIN(TIME(TimePour)) AS StartTime,
    MAX(TIME(TimePour)) AS EndTime,
    SUM(PourStatus) AS TotalPourStatus,
    BatchGroup
FROM (
    SELECT 
        TimePour,
        PatternIdentification,
        PourStatus,

 

        -- Assign Shift Based on Time
        CASE 
            WHEN TIME(TimePour) BETWEEN '07:00:00' AND '18:59:59' THEN 'A'
            ELSE 'B'
        END AS Shift,

 

        -- Adjust Production Date to YYYY-MM-DD Format
        DATE_FORMAT(
            CASE 
                WHEN TIME(TimePour) BETWEEN '07:00:00' AND '18:59:59' 
                THEN DATE(TimePour)  
                ELSE DATE_SUB(DATE(TimePour), INTERVAL 1 DAY)  
            END, '%Y-%m-%d'
        ) AS ProductionDate,

 

        -- Assign Batch Group: Increment when pattern changes
        @BatchGroup := IF(@PrevPattern = PatternIdentification, @BatchGroup, @BatchGroup + 1) AS BatchGroup,
        @PrevPattern := PatternIdentification

    FROM rba_data.moulding_machine_data
    ORDER BY TimePour
) AS RankedData
WHERE STR_TO_DATE(ProductionDate, '%Y-%m-%d') >= '2025-03-07'
GROUP BY ProductionDate, Shift, PatternIdentification, BatchGroup
ORDER BY STR_TO_DATE(ProductionDate, '%Y-%m-%d'), Shift, StartTime;