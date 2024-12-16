SELECT CASE 
            WHEN RTRIM(EQMFCD) = 'AA' 
            AND LEFT(EQMFSN,1) = '0'
            AND LEN(RTRIM(EQMFSN)) = 9
            THEN RIGHT(RTRIM(EQMFSN), 8) 
            ELSE EQMFSN 
           END AS [Serial_Number]
          ,[EQMFSN] AS [Dbs_Serial_Number]
          ,[USGCDE] AS [Smu_Unit]
          ,[USGDA8] AS [Smu_Date]
          ,[CURSMU] AS [SMU]
          ,CASE
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