SELECT
               [SerialNumber] AS [Serial_Number]
               ,CASE 
                 WHEN Manufacturer = 'CATERPILLAR'
                 THEN CONCAT('0',[SerialNumber])
                 ELSE SerialNumber
                END AS [Dbs_Serial_Number]
               ,[DateOpened] AS [Smu_Date]
               ,[SMU]
               ,CASE
                 WHEN [SMUType] = 'Hours' THEN 'H'
                 ELSE [SMUType]
               END AS [Smu_Unit]
               ,'Ci' AS [Source]
           FROM Wagner_ODS.em.CatInspectData
           WHERE SMUType = 'Hours'
           AND SMU <> 1
           AND [DateOpened] BETWEEN DATEADD(YEAR, -1, GETDATE()) AND GETDATE()