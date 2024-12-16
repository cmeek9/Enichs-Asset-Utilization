# import pandas as pd
# import pyodbc
# from datetime import datetime, timedelta
# import os
# from asset_config import sos_tbl, dai_server, prod_server, insp_tbl, dbs_tbl, daily_tbl, em_tbl, asset_util
# import hashlib
# import logging
# import pyodbc
# from sqlalchemy import create_engine
# import numpy as np
# from sqlalchemy import create_engine

# def whitespace_remover(dataframe):
#     dataframe[:] = dataframe.applymap(lambda x: x.strip() if isinstance(x, str) else x)
#     return dataframe

# def sos_smu(dai_server, sos_tbl):
#     connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={dai_server};Trusted_Connection=yes;'
#     with pyodbc.connect(connection_string) as connection:
#         query = f'''
#             SELECT DISTINCT [serialnumber] AS [Serial_Number]
#                             ,[dbsserialnumber] AS [Dbs_Serial_Number]
#                             ,[sampleddate] AS [Smu_Date]
#                             ,[meterreading] AS [SMU]
#                             ,[Sample_Meter_Units]  AS [Smu_Unit]
#                             ,'So' AS [Source]
#             FROM {sos_tbl}
#             WHERE testlist LIKE 'ENG%'
#             AND Sample_Meter_Units = 'H'
#             AND serialnumber NOT LIKE 'LOS%'
#             AND meterreading <> ''
#             AND [Description] NOT LIKE '%REAR'
#             AND [sampleddate] BETWEEN DATEADD(YEAR, -1, GETDATE()) AND GETDATE()
#             '''
        
#         sos_df = pd.read_sql_query(query, connection)
#         sos_df['SMU'] = sos_df['SMU'].astype(float)
#         sos_df['Smu_Date'] = pd.to_datetime(sos_df['Smu_Date'])
#         sos_df['Smu_Date'] = sos_df['Smu_Date'].dt.strftime('%Y-%m-%d')
#         sos_df.drop_duplicates(inplace = True)
#         whitespace_remover(sos_df)
    
#     return sos_df

# df_sos = sos_smu(dai_server, sos_tbl)

# def insp_smu(prod_server, insp_tbl):
#     connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={prod_server};Trusted_Connection=yes;'
#     with pyodbc.connect(connection_string) as connection:
#         query = f'''
#             SELECT
#               [SerialNumber] AS [Serial_Number]
#               ,CASE 
#                 WHEN Manufacturer = 'CATERPILLAR'
#                 THEN CONCAT('0',[SerialNumber])
#                 ELSE SerialNumber
#                END AS [Dbs_Serial_Number]
#               ,[DateOpened] AS [Smu_Date]
#               ,[SMU]
#               ,CASE
#                 WHEN [SMUType] = 'Hours' THEN 'H'
#                 ELSE [SMUType]
#               END AS [Smu_Unit]
#               ,'Ci' AS [Source]
#           FROM {insp_tbl}
#           WHERE SMUType = 'Hours'
#           AND SMU <> 1
#           AND [DateOpened] BETWEEN DATEADD(YEAR, -1, GETDATE()) AND GETDATE()
#             '''
        
#         insp_df = pd.read_sql_query(query, connection)
#         insp_df['Smu_Date'] = pd.to_datetime(insp_df['Smu_Date'])
#         insp_df['Smu_Date'] = insp_df['Smu_Date'].dt.strftime('%Y-%m-%d')
#         insp_df['SMU'] = insp_df['SMU'].astype(float)
#         insp_df.drop_duplicates(inplace = True)
#         whitespace_remover(insp_df)
    
#     return insp_df

# insp_df = insp_smu(prod_server, insp_tbl)

# def dbs_smu(dai_server, dbs_tbl):
#     connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={dai_server};Trusted_Connection=yes;'
#     with pyodbc.connect(connection_string) as connection:
#         query = f'''
#             SELECT CASE 
#         		WHEN RTRIM(EQMFCD) = 'AA' 
#         		AND LEFT(EQMFSN,1) = '0'
#         		AND LEN(RTRIM(EQMFSN)) = 9
#         		THEN RIGHT(RTRIM(EQMFSN), 8) 
#         		ELSE EQMFSN 
#         	   END AS [Serial_Number]
#               ,[EQMFSN] AS [Dbs_Serial_Number]
#               ,[USGCDE] AS [Smu_Unit]
#               ,[USGDA8] AS [Smu_Date]
#               ,[CURSMU] AS [SMU]
#               ,CASE
#                 WHEN RTRIM([INSCCD]) = ''
#                 THEN 'Pm'
#                 ELSE [INSCCD]
#               END AS [Source]
#           FROM {dbs_tbl}
#           WHERE [USGCDE] = 'H'
#           AND EQMFSN <> ''
#           AND EQMFSN NOT LIKE '.%' 
#           AND EQMFSN NOT LIKE '/%' 
#           AND EQMFSN NOT LIKE '00%'
#           AND [LTDSMU] <> 1
#           AND [USGDA8] BETWEEN DATEADD(YEAR, -1, GETDATE()) AND GETDATE()     
#             '''
        
#         dbs_df = pd.read_sql_query(query, connection)
#         dbs_df['Smu_Date'] = pd.to_datetime(dbs_df['Smu_Date'])
#         dbs_df['Smu_Date'] = dbs_df['Smu_Date'].dt.strftime('%Y-%m-%d')
#         dbs_df['SMU'] = dbs_df['SMU'].astype(float)
#         dbs_df.drop_duplicates(inplace = True)
#         whitespace_remover(dbs_df)
    
#     return dbs_df

# dbs_smu_df = dbs_smu(dai_server, dbs_tbl)

# def daily_smu(prod_server, daily_tbl):
#     connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={prod_server};Trusted_Connection=yes;'
#     with pyodbc.connect(connection_string) as connection:
#         query = f'''
#             SELECT 
#         	   [Serial_Number]
#         	   ,CASE
#         		WHEN [Make] = 'CAT'
#         		THEN CONCAT('0',[Serial_Number])
#         		ELSE [Serial_Number]
#         	   END AS [Dbs_Serial_Number]
#                ,[Smu_Reported_Time] AS [Smu_Date]
#                ,[Smu_Value] AS [SMU]
#                ,CASE
#                 WHEN [Smu_Unit] = 'Hours' THEN 'H'
#                 ELSE [Smu_Unit]
#                END AS [Smu_Unit]
#         	  ,'Bd' AS [Source]
#             FROM {daily_tbl}
#             WHERE [Smu_Unit] = 'Hours'
#             AND [Smu_Reported_Time] BETWEEN DATEADD(YEAR, -1, GETDATE()) AND GETDATE()

#             '''
        
#         daily_df = pd.read_sql_query(query, connection)
#         daily_df['Smu_Date'] = pd.to_datetime(daily_df['Smu_Date'])
#         daily_df['Smu_Date'] = daily_df['Smu_Date'].dt.strftime('%Y-%m-%d')
#         daily_df['SMU'] = daily_df['SMU'].astype(float)
#         daily_df.drop_duplicates(inplace = True)
#         whitespace_remover(daily_df)
    
#     return daily_df

# Bd_df = daily_smu(prod_server, daily_tbl)

# def compute_hash(row):
#     # Convert the entire row to a string and encode it to compute the hash
#     hash_str = ''.join(row.astype(str))
#     return hashlib.sha256(hash_str.encode()).hexdigest()

# def process_and_clean_data(df_sos, insp_df, dbs_smu_df, Bd_df):

#     # Merge dataframes
#     merged_df = pd.concat([df_sos, insp_df, dbs_smu_df, Bd_df], ignore_index=True)

#     # Ensure Smu_Date is in datetime format once
#     merged_df['Smu_Date'] = pd.to_datetime(merged_df['Smu_Date'], errors='coerce')
    
#     # Drop rows with NaT in Smu_Date after conversion (if any)
#     merged_df.dropna(subset=['Smu_Date'], inplace=True)
    
#     # Filter data by SMU range
#     merged_df = merged_df[merged_df['SMU'].between(1, 200000)]
    
#     # Track the true minimum Smu_Date for each Serial_Number
#     min_dates = merged_df.groupby('Dbs_Serial_Number')['Smu_Date'].min().reset_index().rename(columns={'Smu_Date': 'Min_Smu_Date'})
    
#     # Keep the row with the latest Smu_Date for each Serial_Number and SMU
#     idx_max = merged_df.groupby(['Dbs_Serial_Number', 'SMU'])['Smu_Date'].idxmax()
#     merged_df = merged_df.loc[idx_max].reset_index(drop=True)

#     # Keep the row with the latest SMU for each Serial_Number and Smu_Date (if needed)
#     idx_max = merged_df.groupby(['Dbs_Serial_Number', 'Smu_Date'])['SMU'].idxmax()
#     merged_df = merged_df.loc[idx_max].reset_index(drop=True)
    
#     # Sort by Serial_Number and Smu_Date (ascending)
#     merged_df.sort_values(by=['Smu_Date', 'SMU'], ascending=[True, True], inplace=True)
    
#     # Calculate SMU and Days differences using vectorized operations
#     merged_df['SMU_Diff'] = merged_df.groupby('Dbs_Serial_Number')['SMU'].diff()
#     merged_df['Days_Diff'] = merged_df.groupby('Dbs_Serial_Number')['Smu_Date'].diff().dt.days
    
#     # Normalize SMU_Diff by Days_Diff to calculate a daily SMU increase
#     merged_df['Daily_SMU'] = merged_df['SMU_Diff'] / merged_df['Days_Diff']
    
#     # Handle cases where Days_Diff is 0 or NaN
#     merged_df['Daily_SMU'].replace([np.inf, -np.inf], np.nan, inplace=True)
#     merged_df['Daily_SMU'].fillna(merged_df['SMU_Diff'], inplace=True)  # If Days_Diff is 0, use SMU_Diff directly
    
#     # Filter out invalid SMU differences, now based on normalized daily SMU
#     valid_diff = (merged_df['Daily_SMU'] >= 0) & (merged_df['Daily_SMU'] <= 24)  # Assuming a max of 24 SMU per day
#     cleaned_df = merged_df[valid_diff].copy()
    
#     # Merge back the true minimum Smu_Date
#     cleaned_df = cleaned_df.merge(min_dates, on='Dbs_Serial_Number', how='left')
    
#     # Compute hash for each row in the cleaned dataset
#     cleaned_df['hash'] = cleaned_df.apply(compute_hash, axis=1)
    
#     return cleaned_df

# cleaned_df = process_and_clean_data(df_sos, insp_df, dbs_smu_df, Bd_df)



# def calculate_average_hours_per_day(df, days_list):
#     # Ensure the Smu_Date is in datetime format
#     df['Smu_Date'] = pd.to_datetime(df['Smu_Date'])

#     # Initialize a dictionary to store results for each window
#     results = {}

#     # Loop over each window of days in days_list
#     for days in days_list:
#         # Calculate the total SMU usage for the last 'days' period for each Serial_Number
#         end_date = df['Smu_Date'].max()  # The most recent date in the dataset
#         start_date = end_date - timedelta(days=days)

#         # Filter the DataFrame for the relevant date range
#         filtered_df = df[(df['Smu_Date'] >= start_date) & (df['Smu_Date'] <= end_date)]

#         # Group by Serial_Number and sum SMU_Diff over the period
#         total_usage = filtered_df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['SMU_Diff'].sum()

#         # Calculate the actual number of days in the period
#         actual_days = filtered_df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['Days_Diff'].sum()

#         # Calculate the average daily usage over the period
#         average_daily_usage = total_usage / actual_days

#         # Store the result
#         results[f'{days}D_Avg_Daily_Usage'] = average_daily_usage

#     # Combine results into a single DataFrame
#     results_df = pd.DataFrame(results)

#     # Round all average daily usage values to 1 decimal place
#     results_df = results_df.round(1)

#     # Add last SMU, Smu_Date, and Source for each Serial_Number
#     last_records = df.sort_values('Smu_Date').groupby(['Serial_Number', 'Dbs_Serial_Number']).last()[['SMU', 'Smu_Date', 'Source']]
#     # results_df = results_df.merge(last_records, on='Serial_Number', how='left')
#     results_df = results_df.merge(last_records, left_index=True, right_index=True, how='left')
    

#     # Calculate average yearly usage
#     min_dates = df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['Min_Smu_Date'].first()
#     max_dates = df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['Smu_Date'].max()

#     # Calculate total usage per Serial_Number
#     total_usage_per_sn = df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['SMU_Diff'].sum()

#     # Calculate the actual days in operation
#     days_in_operation = (max_dates - min_dates).dt.days

#     # Calculate average yearly usage (scaled for a year)
#     avg_yearly_usage = (total_usage_per_sn / days_in_operation) * 365.25

#     # Replace infinity and negative infinity values with NaN
#     avg_yearly_usage.replace([float('inf'), -float('inf')], float('nan'), inplace=True)

#     # Merge Avg_Yearly_Usage into results_df
#     # results_df = results_df.merge(avg_yearly_usage.rename('Avg_Yearly_Usage'), on='Serial_Number', how='left')
#     results_df = results_df.merge(avg_yearly_usage.rename('Avg_Yearly_Usage'), left_index=True, right_index=True, how='left')

#     # Round Avg_Yearly_Usage and Weighted_Avg_Usage to 1 decimal place
#     results_df['Avg_Yearly_Usage'] = results_df['Avg_Yearly_Usage'].round(1)

#     # Calculate the weighted average
#     weights = {
#         '10D_Avg_Daily_Usage': 0.5,
#         '30D_Avg_Daily_Usage': 0.3,
#         '180D_Avg_Daily_Usage': 0.1,
#         '365D_Avg_Daily_Usage': 0.1
#     }
#     results_df['Weighted_Avg_Usage'] = results_df.apply(
#         lambda row: sum(row[col] * weight for col, weight in weights.items() if pd.notna(row[col])),
#         axis=1
#     )

#     # Round the Weighted_Avg_Usage to 1 decimal place
#     results_df['Weighted_Avg_Usage'] = results_df['Weighted_Avg_Usage'].round(1)

#     # Compute hash for each row in the cleaned dataset
#     results_df['hash'] = results_df.apply(compute_hash, axis=1)

#     return results_df.reset_index()

# # Example usage
# days_list = [10, 30, 90, 180, 365]
# average_usage_df = calculate_average_hours_per_day(cleaned_df, days_list)

# # Drop duplicates based on a subset of columns -------------------------------------------------- Need to find a better solution for this
# # Identify the index of the row with the maximum SMU for each Serial_Number and Dbs_Serial_Number
# smu_idx = average_usage_df.groupby(['Dbs_Serial_Number'])['SMU'].idxmax()
# average_usage_df = average_usage_df.loc[smu_idx].reset_index(drop=True)

# from sqlalchemy import create_engine, inspect
# import pandas as pd

# def write_new_data_to_sql(df, table_name, connection_string, chunk_size=1000):
#     # Create a SQLAlchemy engine
#     engine = create_engine(connection_string)
    
#     # Replace the table with the new DataFrame
#     with engine.connect() as connection:
#         df.to_sql(name=table_name, con=engine, if_exists='replace', index=False, chunksize=chunk_size)
#         print(f"Replaced table {table_name} with {len(df)} rows.")

# # Usage:
# connection_string = f'mssql+pyodbc://{prod_server}/{em_tbl}?driver=ODBC+Driver+17+for+SQL+Server'
# table_name = asset_util
# write_new_data_to_sql(average_usage_df, table_name, connection_string, chunk_size=1000)

