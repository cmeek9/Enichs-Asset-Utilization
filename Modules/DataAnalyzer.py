import pandas as pd
from datetime import timedelta
import numpy as np
from Modules.DataHasher import DataHasher
from config import logging

class DataAnalyzer:
    """
    A class for analyzing SMU data and calculating averages.
    """

    def ensure_smu_date_format(df):
        """Ensure Smu_Date is in datetime format."""
        df['Smu_Date'] = pd.to_datetime(df['Smu_Date'])
        return df

    def calculate_average_daily_usage_for_period(df, days, max_daily_usage_threshold=24):
        """Calculate average daily usage for a specific period."""
        end_date = df['Smu_Date'].max()
        start_date = end_date - timedelta(days=days)
        
        # First, get your time window
        filtered_df = df[(df['Smu_Date'] >= start_date) & (df['Smu_Date'] <= end_date)].copy()
        
        # Calculate total SMU difference for the period
        usage_sum = filtered_df.groupby(['Serial_Number','Dbs_Serial_Number'])['SMU'].agg(lambda x: x.max() - x.min())
        
        # Calculate actual days in period
        days_in_period = filtered_df.groupby(['Serial_Number','Dbs_Serial_Number'])['Smu_Date'].agg(
            lambda x: (x.max() - x.min()).days
        )
        
        # Calculate average
        avg_daily_usage = usage_sum / days_in_period
        avg_daily_usage = avg_daily_usage.replace([np.inf, -np.inf], np.nan)
        
        # Apply threshold after averaging
        avg_daily_usage = avg_daily_usage.where(avg_daily_usage <= max_daily_usage_threshold)
        
        avg_daily_usage.name = f'{days}D_Avg_Daily_Usage'
        return avg_daily_usage

    def calculate_average_daily_usage(df, days_list):
        """
        Calculate average daily usage over multiple periods
        and combine them side by side in one DataFrame.
        """
        # For each "days" in days_list, we get a Series of daily usage, then concat
        average_usage_dfs = []
        for days in days_list:
            avg_daily_usage = DataAnalyzer.calculate_average_daily_usage_for_period(df, days)
            average_usage_dfs.append(avg_daily_usage)

        # Combine side by side
        results_df = pd.concat(average_usage_dfs, axis=1)

        # Round to 1 decimal
        results_df = results_df.round(1)
        return results_df

    def add_last_records(df, results_df):
        """Add last SMU, Smu_Date, and Source for each (Serial_Number, Dbs_Serial_Number)."""
        # Sort by Smu_Date, then grab the last row per group
        last_records = df.sort_values('Smu_Date').groupby(['Serial_Number', 'Dbs_Serial_Number']).last()

        # Merge those columns onto results_df
        results_df = results_df.merge(
            last_records[['SMU', 'Smu_Date', 'Source']],
            left_index=True,
            right_index=True,
            how='left'
        )
        return results_df

    def calculate_average_yearly_usage(df):
        """
        Calculate average yearly usage.
        If you do NOT have a 'Min_Smu_Date' column, either:
            - create it, or
            - adapt to find min from `Smu_Date`.
        """
        # Example: if your df doesn't have Min_Smu_Date, define it:
        if 'Min_Smu_Date' not in df.columns:
            df['Min_Smu_Date'] = df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['Smu_Date'].transform('min')

        grouped = df.groupby(['Serial_Number', 'Dbs_Serial_Number']).agg({
            'Min_Smu_Date': 'first',
            'Smu_Date': 'max',
            'SMU_Diff': 'sum'
        })

        # Number of days in operation
        days_in_operation = (grouped['Smu_Date'] - grouped['Min_Smu_Date']).dt.days

        # Usage per year
        avg_yearly_usage = (grouped['SMU_Diff'] / days_in_operation) * 365.25
        avg_yearly_usage = avg_yearly_usage.replace([np.inf, -np.inf], np.nan)  # Just to be safe

        # Name and round
        avg_yearly_usage.name = 'Avg_Yearly_Usage'
        avg_yearly_usage = avg_yearly_usage.round(1)

        return avg_yearly_usage

    def merge_avg_yearly_usage(results_df, avg_yearly_usage):
        """Merge the yearly usage column into results_df on index (Serial_Number, Dbs_Serial_Number)."""
        results_df = results_df.merge(avg_yearly_usage, left_index=True, right_index=True, how='left')
        return results_df

    def sanitize_data(df):
        """Replace infinity and negative infinity values with NaN."""
        return df.replace([np.inf, -np.inf], np.nan)

    def calculate_weighted_average_usage(results_df, weights):
        """Calculate weighted average usage based on specified weights."""
        def weighted_avg(row):
            total = 0.0
            for col, weight in weights.items():
                val = row.get(col, np.nan)
                if pd.notna(val):
                    total += val * weight
            return total

        results_df['Weighted_Avg_Usage'] = results_df.apply(weighted_avg, axis=1)
        results_df['Weighted_Avg_Usage'] = results_df['Weighted_Avg_Usage'].round(1)
        return results_df

    def calculate_average_hours_per_day(df, days_list):
        """
        This is the “master” method that calls all sub-steps to replicate the old single-function logic.
        """
        try:
            # 1. Ensure datetime
            df = DataAnalyzer.ensure_smu_date_format(df)
            logging.info('Ensuring date format.')
            # 2. Calculate multi-period daily usage
            results_df = DataAnalyzer.calculate_average_daily_usage(df, days_list)
            logging.info('Calculated average daily use.')

            # 3. Add last-record data
            results_df = DataAnalyzer.add_last_records(df, results_df)
            logging.info('added last record.')

            # 4. Compute average yearly usage, then merge
            avg_yearly_usage = DataAnalyzer.calculate_average_yearly_usage(df)
            logging.info('Calculated average yearly use.')
            results_df = DataAnalyzer.merge_avg_yearly_usage(results_df, avg_yearly_usage)
            logging.info('merge avergae yearly use.')

            # 5. Weighted average
            weights = {
                '10D_Avg_Daily_Usage': 0.5,
                '30D_Avg_Daily_Usage': 0.3,
                '180D_Avg_Daily_Usage': 0.1,
                '365D_Avg_Daily_Usage': 0.1
            }
            results_df = DataAnalyzer.calculate_weighted_average_usage(results_df, weights)
            logging.info('calculated weighted averages.')

            # 6. Sanitize data
            results_df = DataAnalyzer.sanitize_data(results_df)
            logging.info('sanitized data.')

            # 7. Compute hash for each row (if that’s how you do it)
            results_df = DataHasher.compute_hashes(results_df)
            logging.info('hashed records.')

            # 8. Return final
            return results_df.reset_index()

        except Exception as e:
            logging.error(str(e))
            raise



    # def calculate_weighted_average_usage(results_df, weights):
    #     def weighted_avg(row):
    #         val_sum = 0
    #         w_sum = 0
    #         for col, weight in weights.items():
    #             if pd.notna(row.get(col, np.nan)):
    #                 val_sum += row[col] * weight
    #                 w_sum += weight
    #         if w_sum == 0:
    #             return np.nan
    #         return val_sum

    #     results_df['Weighted_Avg_Usage'] = results_df.apply(weighted_avg, axis=1)
    #     results_df['Weighted_Avg_Usage'] = results_df['Weighted_Avg_Usage'].round(1)
    #     return results_df




# import pandas as pd
# import numpy as np
# from datetime import timedelta
# import hashlib
# from config import logging

# class DataAnalyzer:
#     """
#     A class holding data-analysis functionality.
#     """

#     @staticmethod
#     def compute_hash(row):
#         # If you need the same hashing function, ensure consistency
#         row_str = ''.join(str(value) for value in row.values)
#         return hashlib.md5(row_str.encode('utf-8')).hexdigest()

#     @staticmethod
#     def calculate_average_hours_per_day(df, days_list):
#         """
#         Calculate average SMU usage over multiple day windows (e.g., 10, 30, 180, 365).
#         """
#         # Ensure the Smu_Date is in datetime format
#         df['Smu_Date'] = pd.to_datetime(df['Smu_Date'])

#         # Initialize a dictionary to store results for each window
#         results = {}

#         # Loop over each window of days in days_list
#         for days in days_list:
#             end_date = df['Smu_Date'].max()
#             start_date = end_date - timedelta(days=days)

#             # Filter the DataFrame for the date range
#             filtered_df = df[(df['Smu_Date'] >= start_date) & (df['Smu_Date'] <= end_date)]

#             # Group by Serial_Number and sum SMU_Diff
#             total_usage = filtered_df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['SMU_Diff'].sum()

#             # Group by Serial_Number and sum Days_Diff
#             actual_days = filtered_df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['Days_Diff'].sum()

#             # Calculate the average daily usage
#             average_daily_usage = total_usage / actual_days

#             # Store the result
#             results[f'{days}D_Avg_Daily_Usage'] = average_daily_usage

#         # Combine results into a single DataFrame
#         results_df = pd.DataFrame(results).round(1)

#         # Add last SMU, Smu_Date, and Source for each Serial_Number
#         last_records = (df
#                         .sort_values('Smu_Date')
#                         .groupby(['Serial_Number', 'Dbs_Serial_Number'])
#                         .last()[['SMU', 'Smu_Date', 'Source']])
        
#         # Merge into results_df
#         results_df = results_df.merge(last_records, left_index=True, right_index=True, how='left')

#         # Calculate average yearly usage
#         min_dates = df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['Min_Smu_Date'].first()
#         max_dates = df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['Smu_Date'].max()
#         total_usage_per_sn = df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['SMU_Diff'].sum()
        
#         # Days in operation
#         days_in_operation = (max_dates - min_dates).dt.days
#         avg_yearly_usage = (total_usage_per_sn / days_in_operation) * 365.25
#         avg_yearly_usage.replace([float('inf'), -float('inf')], float('nan'), inplace=True)

#         # Merge Avg_Yearly_Usage into results_df
#         results_df = results_df.merge(avg_yearly_usage.rename('Avg_Yearly_Usage'), 
#                                       left_index=True, right_index=True, 
#                                       how='left')
#         results_df['Avg_Yearly_Usage'] = results_df['Avg_Yearly_Usage'].round(1)

#         # Calculate weighted average usage
#         weights = {
#             '10D_Avg_Daily_Usage':  0.5,
#             '30D_Avg_Daily_Usage':  0.3,
#             '180D_Avg_Daily_Usage': 0.1,
#             '365D_Avg_Daily_Usage': 0.1
#         }

#         results_df['Weighted_Avg_Usage'] = results_df.apply(
#             lambda row: sum(
#                 row[col] * weight
#                 for col, weight in weights.items()
#                 if pd.notna(row[col])
#             ),
#             axis=1
#         ).round(1)

#         # Compute a unique hash for each row
#         results_df['hash'] = results_df.apply(DataAnalyzer.compute_hash, axis=1)

#         return results_df.reset_index()

#     def analyze_data(self, cleaned_df, days_list=None):
#         """
#         Perform the analysis steps on cleaned data.
#         """
#         if days_list is None:
#             days_list = [10, 30, 180, 365]  # default day windows

#         try:
#             analysis_df = DataAnalyzer.calculate_average_hours_per_day(cleaned_df, days_list)
#             logging.info("Calculated average hours per day for multiple windows.")
#             return analysis_df
#         except Exception as e:
#             logging.error(f"Error in analyze_data: {str(e)}")
#             raise