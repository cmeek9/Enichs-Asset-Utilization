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
        df['Smu_Date'] = pd.to_datetime(df['Smu_Date'])
        return df

    def calculate_average_daily_usage_for_period(df, days, max_daily_usage_threshold=24.0):
        # Since anomalies are handled already, just filter the period and average
        end_date = df['Smu_Date'].max()
        start_date = end_date - timedelta(days=days)

        filtered_df = df[(df['Smu_Date'] >= start_date) & (df['Smu_Date'] <= end_date)].copy()
        filtered_df = filtered_df[filtered_df['Daily_SMU'].notnull() & (filtered_df['Daily_SMU'] <= max_daily_usage_threshold)]

        if filtered_df.empty:
            return pd.Series(dtype=float, name=f'{days}D_Avg_Daily_Usage')

        # Directly compute the mean since data is already cleaned and outliers handled upstream
        grouped = filtered_df.groupby(['Serial_Number', 'Dbs_Serial_Number'])['Daily_SMU'].mean()
        grouped.name = f'{days}D_Avg_Daily_Usage'
        return grouped


    def calculate_average_daily_usage(df, days_list):
        average_usage_dfs = []
        for days in days_list:
            avg_daily_usage = DataAnalyzer.calculate_average_daily_usage_for_period(
                df, 
                days, 
                max_daily_usage_threshold=24.0
            )
            average_usage_dfs.append(avg_daily_usage)

        if average_usage_dfs:
            results_df = pd.concat(average_usage_dfs, axis=1)
            results_df = results_df.round(1)
            return results_df
        else:
            return pd.DataFrame()


    def add_last_records(df, results_df):
        last_records = df.sort_values('Smu_Date').groupby(['Serial_Number', 'Dbs_Serial_Number']).last()[['SMU', 'Smu_Date', 'Source']]
        results_df = results_df.merge(last_records, left_index=True, right_index=True, how='left')
        return results_df

    def calculate_average_yearly_usage(df):
        grouped = df.groupby(['Serial_Number', 'Dbs_Serial_Number']).agg({
            'Min_Smu_Date': 'first',
            'Smu_Date': 'max',
            'SMU_Diff': 'sum'
        })
        days_in_operation = (grouped['Smu_Date'] - grouped['Min_Smu_Date']).dt.days
        days_in_operation = days_in_operation.replace(0, np.nan)
        avg_yearly_usage = (grouped['SMU_Diff'] / days_in_operation) * 365.25
        avg_yearly_usage = avg_yearly_usage.round(1)
        avg_yearly_usage.name = 'Avg_Yearly_Usage'
        return avg_yearly_usage

    def merge_avg_yearly_usage(results_df, avg_yearly_usage):
        return results_df.merge(avg_yearly_usage, left_index=True, right_index=True, how='left')

    def sanitize_data(df):
        sanitized_df = df.replace([float('inf'), -float('inf')], float('nan'))
        return sanitized_df

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

    def calculate_average_hours_per_day(df, days_list):
        try:
            df = DataAnalyzer.ensure_smu_date_format(df)
            logging.info('Ensuring date format.')
            results_df = DataAnalyzer.calculate_average_daily_usage(df, days_list)
            logging.info('Calculated median-based average daily usage with trimming outliers.')
            results_df = DataAnalyzer.add_last_records(df, results_df)
            logging.info('Added last records.')
            avg_yearly_usage = DataAnalyzer.calculate_average_yearly_usage(df)
            logging.info('Yearly average calculated.')
            results_df = DataAnalyzer.merge_avg_yearly_usage(results_df, avg_yearly_usage)
            logging.info('Merged yearly usage.')
            # weights = {
            #     '10D_Avg_Daily_Usage': 0.5,
            #     '30D_Avg_Daily_Usage': 0.3,
            #     '180D_Avg_Daily_Usage': 0.05,
            #     '365D_Avg_Daily_Usage': 0.05
            # }
            # results_df = DataAnalyzer.calculate_weighted_average_usage(results_df, weights)
            # logging.info('Calculated weighted averages.')
            results_df = DataAnalyzer.sanitize_data(results_df)
            logging.info('Sanitized DataFrame by replacing infinity values with NaN.')
            results_df = DataHasher.compute_hashes(results_df)
            logging.info('All average calculations completed successfully.')
            return results_df.reset_index()
        except Exception as e:
            logging.error(str(e))
            return pd.DataFrame()
