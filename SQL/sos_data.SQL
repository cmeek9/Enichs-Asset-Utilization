SELECT DISTINCT [serialnumber] AS [Serial_Number]
                             ,[dbsserialnumber] AS [Dbs_Serial_Number]
                             ,[sampleddate] AS [Smu_Date]
                             ,[meterreading] AS [SMU]
                             ,[Sample_Meter_Units]  AS [Smu_Unit]
                             ,'So' AS [Source]
             FROM Cloudlink.ddt.EM_SOS_Samples_Wide
             WHERE testlist LIKE 'ENG%'
             AND Sample_Meter_Units = 'H'
             AND serialnumber NOT LIKE 'LOS%'
             AND meterreading <> ''
             AND [Description] NOT LIKE '%REAR'
             AND [sampleddate] BETWEEN DATEADD(YEAR, -1, GETDATE()) AND GETDATE()