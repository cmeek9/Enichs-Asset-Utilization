SELECT 
    [Serial_Number]
    ,CASE
		WHEN [Make] = 'CAT' THEN CONCAT('0',[Serial_Number])
		ELSE [Serial_Number]
    END AS [Dbs_Serial_Number]
    ,[Smu_Reported_Time] AS [Smu_Date]
    ,[Smu_Value] AS [SMU]
    ,CASE
        WHEN [Smu_Unit] = 'Hours' THEN 'H'
        ELSE [Smu_Unit]
    END AS [Smu_Unit]
    ,'Bd' AS [Source]
FROM Wagner_ODS.em.BasicDailyLocationSMU
WHERE [Smu_Unit] = 'Hours'
AND [Smu_Reported_Time] BETWEEN DATEADD(YEAR, -1, GETDATE()) AND GETDATE()