WITH AllData AS (
    -- 1st query
    SELECT 
        CASE 
            WHEN RTRIM(EQMFCD) = 'AA' 
                AND LEFT(EQMFSN,1) = '0'
                AND LEN(RTRIM(EQMFSN)) = 9
            THEN RIGHT(RTRIM(EQMFSN), 8) 
            ELSE EQMFSN 
        END AS [Serial_Number],
        [EQMFSN] AS [Dbs_Serial_Number],
        [USGDA8] AS [Smu_Date],
        [CURSMU] AS [SMU],
        [USGCDE] AS [Smu_Unit],
        CASE
            WHEN RTRIM([INSCCD]) = ''
            THEN 'Pm'
            ELSE [INSCCD]
        END AS [Source]
    FROM LIBE25.dbo.PMPESMD0
    WHERE [USGCDE] = 'H'
        AND EQMFSN <> ''
        AND EQMFSN NOT LIKE '.%' 
        AND EQMFSN NOT LIKE '/%' 
        AND EQMFSN NOT LIKE '00%'
        AND [LTDSMU] <> 1
        AND RTRIM([INSCCD]) NOT IN ('D','V')
        AND [USGDA8] BETWEEN DATEADD(YEAR, -1, GETDATE()) AND GETDATE()
    
    UNION ALL
    
    -- 2nd query
    SELECT DISTINCT 
        [serialnumber] AS [Serial_Number],
        [dbsserialnumber] AS [Dbs_Serial_Number],
        [sampleddate] AS [Smu_Date],
        CAST([meterreading] AS DECIMAL(18,2)) AS [SMU],
        [Sample_Meter_Units]  AS [Smu_Unit],
        'So' AS [Source]
    FROM Cloudlink.ddt.EM_SOS_Samples_Wide
    WHERE testlist LIKE 'ENG%'
        AND Sample_Meter_Units = 'H'
        AND serialnumber NOT LIKE 'LOS%'
        AND meterreading <> ''
        AND [Description] NOT LIKE '%REAR'
        AND [sampleddate] BETWEEN DATEADD(YEAR, -1, GETDATE()) AND GETDATE()
    
    UNION ALL
    
    -- 3rd query
    SELECT
        [SerialNumber] AS [Serial_Number],
        CASE 
            WHEN Manufacturer = 'CATERPILLAR'
            THEN CONCAT('0', [SerialNumber])
            ELSE SerialNumber
        END AS [Dbs_Serial_Number],
        [DateOpened] AS [Smu_Date],
        [SMU],
        CASE
            WHEN [SMUType] = 'Hours' THEN 'H'
            ELSE [SMUType]
        END AS [Smu_Unit],
        'Ci' AS [Source]
    FROM [WagnerProdAGL1].Wagner_ODS.em.CatInspectData
    WHERE SMUType = 'Hours'
        AND SMU <> 1
        AND [DateOpened] BETWEEN DATEADD(YEAR, -1, GETDATE()) AND GETDATE()
    
    UNION ALL
    
    -- 4th query
    SELECT DISTINCT
        [Serial_Number],
        CASE
            WHEN [Make] = 'CAT' THEN CONCAT('0', [Serial_Number])
            ELSE [Serial_Number]
        END AS [Dbs_Serial_Number],
        [Smu_Reported_Time] AS [Smu_Date],
        [Smu_Value] AS [SMU],
        CASE
            WHEN [Smu_Unit] = 'Hours' THEN 'H'
            ELSE [Smu_Unit]
        END AS [Smu_Unit],
        'Bd' AS [Source]
    FROM [WagnerProdAGL1].Wagner_ODS.em.BasicDailyLocationSMU
    WHERE [Smu_Unit] = 'Hours'
        AND [Smu_Reported_Time] BETWEEN DATEADD(YEAR, -1, GETDATE()) AND GETDATE()
)
SELECT 
    [Serial_Number],
    [Dbs_Serial_Number],
    [Smu_Date],
    [SMU],
    [Smu_Unit],
    [Source]
FROM AllData

