# OLD CODE FOR REFERENCE, BASED ON ENICHS ORIGINAL CODE  THEN MODIFIED INTO CLASSES

# import pandas as pd
# import numpy as np
# import hashlib
# from datetime import timedelta

# # Cleaning and Normalizing the data 


# def calculate_differences(df):
#     """Calculate SMU and Days differences."""
#     df = df.copy()
#     df['SMU_Diff'] = df['SMU'].diff().where(df['Dbs_Serial_Number'].eq(df['Dbs_Serial_Number'].shift()))
#     df['Days_Diff'] = df['Smu_Date'].diff().where(df['Dbs_Serial_Number'].eq(df['Dbs_Serial_Number'].shift())).dt.days
#     return df

# def normalize_smu_differences(df):
#     """Normalize SMU differences to get Daily_SMU."""
#     df = df.copy()
#     df['Daily_SMU'] = df['SMU_Diff'] / df['Days_Diff']
#     df.loc[df['Daily_SMU'].isin([np.inf, -np.inf]), 'Daily_SMU'] = np.nan
#     df.loc[df['Daily_SMU'].isna(), 'Daily_SMU'] = df['SMU_Diff']
#     return df

# def filter_valid_rows(df):
#     """Filter rows where Daily_SMU is between 0 and 24."""
#     valid_diff = (df['Daily_SMU'] >= 0) & (df['Daily_SMU'] <= 24)
#     return df[valid_diff]

# def add_min_smu_date(df, min_dates):
#     """Add the true minimum Smu_Date to the DataFrame."""
#     return df.merge(min_dates, left_on='Dbs_Serial_Number', right_index=True, how='left')

# ## main cleaning orchestrator

# def process_and_clean_data(full_df):
#     """Process and clean the data."""
#     full_df = filter_smu_range(full_df)
#     full_df = sort_dataframe(full_df)
#     min_dates = get_min_smu_dates(full_df)
#     full_df = keep_latest_smu_date(full_df)
#     full_df = keep_latest_smu(full_df)
#     full_df = calculate_differences(full_df)
#     full_df = normalize_smu_differences(full_df)
#     cleaned_df = filter_valid_rows(full_df)
#     cleaned_df = add_min_smu_date(cleaned_df, min_dates)
#     return cleaned_df

# # Doing average and annual calculations

# def ensure_smu_date_format(df):
#     """Ensure Smu_Date is in datetime format."""
#     df['Smu_Date'] = pd.to_datetime(df['Smu_Date'])
#     return df

# def calculate_average_daily_usage_for_period(df, days):
#     """Calculate average daily usage for a specific period."""
#     end_date = df['Smu_Date'].max()
#     start_date = end_date - timedelta(days=days)
    
#     # Filtering with boolean indexing
#     filtered_df = df[(df['Smu_Date'] >= start_date) & (df['Smu_Date'] <= end_date)]
    
#     # Combine groupby operations more efficiently
#     grouped = filtered_df.groupby(['Serial_Number', 'Dbs_Serial_Number']).agg({
#         'SMU_Diff': 'sum', 
#         'Days_Diff': 'sum'
#     })
    
#     # Vectorized division
#     average_daily_usage = grouped['SMU_Diff'] / grouped['Days_Diff']
#     average_daily_usage.name = f'{days}D_Avg_Daily_Usage'
#     return average_daily_usage

# def calculate_average_daily_usage(df, days_list):
#     """Calculate average daily usage over multiple periods."""
#     average_usage_dfs = []
#     for days in days_list:
#         avg_daily_usage = calculate_average_daily_usage_for_period(df, days)
#         average_usage_dfs.append(avg_daily_usage)
#     results_df = pd.concat(average_usage_dfs, axis=1)
#     results_df = results_df.round(1)
#     return results_df

# def add_last_records(df, results_df):
#     """Add last SMU, Smu_Date, and Source for each Serial_Number."""
#     last_records = df.sort_values('Smu_Date').groupby(['Serial_Number', 'Dbs_Serial_Number']).last()[['SMU', 'Smu_Date', 'Source']]
#     results_df = results_df.merge(last_records, left_index=True, right_index=True, how='left')
#     return results_df

# def calculate_average_yearly_usage(df):
#     """Calculate average yearly usage for each Serial_Number more efficiently."""
#     # Combine date and usage calculations
#     grouped = df.groupby(['Serial_Number', 'Dbs_Serial_Number']).agg({
#         'Min_Smu_Date': 'first',
#         'Smu_Date': 'max',
#         'SMU_Diff': 'sum'
#     })
    
#     # Vectorized calculation
#     days_in_operation = (grouped['Smu_Date'] - grouped['Min_Smu_Date']).dt.days
#     avg_yearly_usage = (grouped['SMU_Diff'] / days_in_operation) * 365.25
    
#     avg_yearly_usage = avg_yearly_usage.round(1)
#     avg_yearly_usage.name = 'Avg_Yearly_Usage'
#     return avg_yearly_usage

# def merge_avg_yearly_usage(results_df, avg_yearly_usage):
#     """Merge average yearly usage into the results DataFrame."""
#     return results_df.merge(avg_yearly_usage, left_index=True, right_index=True, how='left')

# def calculate_weighted_average_usage(results_df, weights):
#     """Calculate weighted average usage based on specified weights."""
#     def weighted_avg(row):
#         return sum(row[col] * weight for col, weight in weights.items() if pd.notna(row.get(col, None)))
#     results_df['Weighted_Avg_Usage'] = results_df.apply(weighted_avg, axis=1)
#     results_df['Weighted_Avg_Usage'] = results_df['Weighted_Avg_Usage'].round(1)
#     return results_df


# ## main avg calculation orchestrator

# def calculate_average_hours_per_day(df, days_list):
#     """Calculate average hours per day."""
#     df = ensure_smu_date_format(df)
#     results_df = calculate_average_daily_usage(df, days_list)
#     results_df = add_last_records(df, results_df)
#     avg_yearly_usage = calculate_average_yearly_usage(df)
#     results_df = merge_avg_yearly_usage(results_df, avg_yearly_usage)
#     weights = {
#         '10D_Avg_Daily_Usage': 0.5,
#         '30D_Avg_Daily_Usage': 0.3,
#         '180D_Avg_Daily_Usage': 0.1,
#         '365D_Avg_Daily_Usage': 0.1
#     }
#     results_df = calculate_weighted_average_usage(results_df, weights)
#     results_df = compute_hashes(results_df)
#     return results_df.reset_index()




# # ###### HELPER FUNCTIONS 

# def compute_hashes(results_df):
#     """Compute hash for each row in the results DataFrame."""
#     results_df['hash'] = results_df.apply(compute_hash, axis=1)
#     return results_df

# def compute_hash(row):
#     # Convert the entire row to a string and encode it to compute the hash
#     hash_str = ''.join(row.astype(str))
#     return hashlib.sha256(hash_str.encode()).hexdigest()

# def filter_smu_range(df):
#     """Filter data by SMU range."""
#     return df[df['SMU'].between(1, 200000)]

# def sort_dataframe(df):
#     """Sort the DataFrame to optimize groupby operations."""
#     return df.sort_values(by=['Dbs_Serial_Number', 'SMU', 'Smu_Date'], ascending=[True, True, True])

# def get_min_smu_dates(df):
#     """Get the true minimum Smu_Date for each Dbs_Serial_Number."""
#     min_dates = df.groupby('Dbs_Serial_Number', sort=False)['Smu_Date'].min()
#     min_dates.name = 'Min_Smu_Date'
#     return min_dates

# def keep_latest_smu_date(df):
#     """Keep the latest Smu_Date for each Dbs_Serial_Number and SMU."""
#     idx_max_smu = df.groupby(['Dbs_Serial_Number', 'SMU'], sort=False)['Smu_Date'].idxmax()
#     return df.loc[idx_max_smu]

# def keep_latest_smu(df):
#     """Keep the latest SMU for each Dbs_Serial_Number and Smu_Date."""
#     idx_max_date = df.groupby(['Dbs_Serial_Number', 'Smu_Date'], sort=False)['SMU'].idxmax()
#     return df.loc[idx_max_date]




# # ------- REF / OLD CODE  -------


# # def process_and_clean_data(df_sos, insp_df, dbs_smu_df, Bd_df):

# #     # Merge dataframes
# #     merged_df = pd.concat([df_sos, insp_df, dbs_smu_df, Bd_df], ignore_index=True)

# #     # Ensure Smu_Date is in datetime format once
# #     merged_df['Smu_Date'] = pd.to_datetime(merged_df['Smu_Date'], errors='coerce')
    
# #     # Drop rows with NaT in Smu_Date after conversion (if any)
# #     merged_df.dropna(subset=['Smu_Date'], inplace=True)
    
# #     # Filter data by SMU range
# #     merged_df = merged_df[merged_df['SMU'].between(1, 200000)]
    
# #     # Track the true minimum Smu_Date for each Serial_Number
# #     min_dates = merged_df.groupby('Dbs_Serial_Number')['Smu_Date'].min().reset_index().rename(columns={'Smu_Date': 'Min_Smu_Date'})
    
# #     # Keep the row with the latest Smu_Date for each Serial_Number and SMU
# #     idx_max = merged_df.groupby(['Dbs_Serial_Number', 'SMU'])['Smu_Date'].idxmax()
# #     merged_df = merged_df.loc[idx_max].reset_index(drop=True)

# #     # Keep the row with the latest SMU for each Serial_Number and Smu_Date (if needed)
# #     idx_max = merged_df.groupby(['Dbs_Serial_Number', 'Smu_Date'])['SMU'].idxmax()
# #     merged_df = merged_df.loc[idx_max].reset_index(drop=True)
    
# #     # Sort by Serial_Number and Smu_Date (ascending)
# #     merged_df.sort_values(by=['Smu_Date', 'SMU'], ascending=[True, True], inplace=True)
    
# #     # Calculate SMU and Days differences using vectorized operations
# #     merged_df['SMU_Diff'] = merged_df.groupby('Dbs_Serial_Number')['SMU'].diff()
# #     merged_df['Days_Diff'] = merged_df.groupby('Dbs_Serial_Number')['Smu_Date'].diff().dt.days
    
# #     # Normalize SMU_Diff by Days_Diff to calculate a daily SMU increase
# #     merged_df['Daily_SMU'] = merged_df['SMU_Diff'] / merged_df['Days_Diff']
    
# #     # Handle cases where Days_Diff is 0 or NaN
# #     merged_df['Daily_SMU'].replace([np.inf, -np.inf], np.nan, inplace=True)
# #     merged_df['Daily_SMU'].fillna(merged_df['SMU_Diff'], inplace=True)  # If Days_Diff is 0, use SMU_Diff directly
    
# #     # Filter out invalid SMU differences, now based on normalized daily SMU
# #     valid_diff = (merged_df['Daily_SMU'] >= 0) & (merged_df['Daily_SMU'] <= 24)  # Assuming a max of 24 SMU per day
# #     cleaned_df = merged_df[valid_diff].copy()
    
# #     # Merge back the true minimum Smu_Date
# #     cleaned_df = cleaned_df.merge(min_dates, on='Dbs_Serial_Number', how='left')
    
# #     # Compute hash for each row in the cleaned dataset
# #     cleaned_df['hash'] = cleaned_df.apply(compute_hash, axis=1)
    
# #     return cleaned_df

# #  -----------------------------------------------------------

# # def calculate_average_hours_per_day(df, days_list):
# #     # Ensure the Smu_Date is in datetime format
# #     df['Smu_Date'] = pd.to_datetime(df['Smu_Date'])

# #     # Initialize a dictionary to store results for each window
# #     results = {}

# #     # Loop over each window of days in days_list
# #     for days in days_list:
# #         # Calculate the total SMU usage for the last 'days' period for each Serial_Number
# #         end_date = df['Smu_Date'].max()  # The most recent date in the dataset
# #         start_date = end_date - timedelta(days=days)

# #         # Filter the DataFrame for the relevant date range
# #         filtered_df = df[(df['Smu_Date'] >= start_date) & (df['Smu_Date'] <= end_date)]

# #         # Group by Serial_Number and sum SMU_Diff over the period
# #         total_usage = filtered_df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['SMU_Diff'].sum()

# #         # Calculate the actual number of days in the period
# #         actual_days = filtered_df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['Days_Diff'].sum()

# #         # Calculate the average daily usage over the period
# #         average_daily_usage = total_usage / actual_days

# #         # Store the result
# #         results[f'{days}D_Avg_Daily_Usage'] = average_daily_usage

# #     # Combine results into a single DataFrame
# #     results_df = pd.DataFrame(results)

# #     # Round all average daily usage values to 1 decimal place
# #     results_df = results_df.round(1)

# #     # Add last SMU, Smu_Date, and Source for each Serial_Number
# #     last_records = df.sort_values('Smu_Date').groupby(['Serial_Number', 'Dbs_Serial_Number']).last()[['SMU', 'Smu_Date', 'Source']]
# #     # results_df = results_df.merge(last_records, on='Serial_Number', how='left')
# #     results_df = results_df.merge(last_records, left_index=True, right_index=True, how='left')
    

# #     # Calculate average yearly usage
# #     min_dates = df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['Min_Smu_Date'].first()
# #     max_dates = df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['Smu_Date'].max()

# #     # Calculate total usage per Serial_Number
# #     total_usage_per_sn = df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['SMU_Diff'].sum()

# #     # Calculate the actual days in operation
# #     days_in_operation = (max_dates - min_dates).dt.days

# #     # Calculate average yearly usage (scaled for a year)
# #     avg_yearly_usage = (total_usage_per_sn / days_in_operation) * 365.25

# #     # Replace infinity and negative infinity values with NaN
# #     avg_yearly_usage.replace([float('inf'), -float('inf')], float('nan'), inplace=True)

# #     # Merge Avg_Yearly_Usage into results_df
# #     # results_df = results_df.merge(avg_yearly_usage.rename('Avg_Yearly_Usage'), on='Serial_Number', how='left')
# #     results_df = results_df.merge(avg_yearly_usage.rename('Avg_Yearly_Usage'), left_index=True, right_index=True, how='left')

# #     # Round Avg_Yearly_Usage and Weighted_Avg_Usage to 1 decimal place
# #     results_df['Avg_Yearly_Usage'] = results_df['Avg_Yearly_Usage'].round(1)

# #     # Calculate the weighted average
# #     weights = {
# #         '10D_Avg_Daily_Usage': 0.5,
# #         '30D_Avg_Daily_Usage': 0.3,
# #         '180D_Avg_Daily_Usage': 0.1,
# #         '365D_Avg_Daily_Usage': 0.1
# #     }
# #     results_df['Weighted_Avg_Usage'] = results_df.apply(
# #         lambda row: sum(row[col] * weight for col, weight in weights.items() if pd.notna(row[col])),
# #         axis=1
# #     )

# #     # Round the Weighted_Avg_Usage to 1 decimal place
# #     results_df['Weighted_Avg_Usage'] = results_df['Weighted_Avg_Usage'].round(1)

# #     # Compute hash for each row in the cleaned dataset
# #     results_df['hash'] = results_df.apply(compute_hash, axis=1)

# #     return results_df.reset_index()