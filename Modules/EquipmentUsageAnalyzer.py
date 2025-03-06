import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config import logging

class EquipmentUsageAnalyzer:
    def __init__(self, max_daily_hours=24):
        self.max_daily_hours = max_daily_hours
        self.weights = {
            '10D_Avg_Daily_Usage': 0.5,
            '30D_Avg_Daily_Usage': 0.3,
            '180D_Avg_Daily_Usage': 0.1,
            '365D_Avg_Daily_Usage': 0.1
        }
        logging.info(f"Initialized EquipmentUsageAnalyzer with max_daily_hours={max_daily_hours}")

    def preprocess_chunk(self, chunk):
        try:
            chunk = chunk.groupby(['Serial_Number', 'Smu_Date']).agg({
                'SMU': 'max',
                'Dbs_Serial_Number': 'last',
                'Source': 'last'
            }).reset_index()
            chunk = chunk.sort_values(['Serial_Number', 'Smu_Date'])
            chunk['prev_date'] = chunk.groupby('Serial_Number')['Smu_Date'].shift(1)
            chunk['prev_smu'] = chunk.groupby('Serial_Number')['SMU'].shift(1)
            chunk['time_diff'] = (chunk['Smu_Date'] - chunk['prev_date']).dt.total_seconds() / 86400
            chunk['smu_diff'] = chunk['SMU'] - chunk['prev_smu']
            chunk['is_suspicious'] = chunk['smu_diff'] < 0
            return chunk[['Serial_Number', 'Dbs_Serial_Number', 'SMU', 'Smu_Date', 'Source', 
                          'smu_diff', 'time_diff', 'is_suspicious']]
        except Exception as e:
            logging.error(f"Error in preprocess_chunk: {str(e)}")
            raise

    def analyze_usage(self, df, reference_date=None, chunk_size=1000):
        try:
            if reference_date is None:
                reference_date = datetime.now()  # e.g., 2025-03-05
            logging.info(f"Starting analysis with {len(df)} rows, reference_date={reference_date}")

            df = df.drop_duplicates(['Serial_Number', 'Smu_Date', 'SMU'])
            logging.info(f"After deduplication: {len(df)} rows")

            chunks = [g for _, g in df.groupby(np.arange(len(df)) // chunk_size)]
            processed_df = pd.concat([self.preprocess_chunk(chunk) for chunk in chunks], ignore_index=True)

            windows = {'10D': 10, '30D': 30, '90D': 90, '180D': 180, '365D': 365}
            results = []
            max_rate_per_day = 24  # Allow legit high usage up to 24 hours/day

            for serial, group in processed_df.groupby('Serial_Number'):
                if len(group) < 1:
                    continue

                group = group.sort_values('Smu_Date')
                stats = {
                    'Serial_Number': serial,
                    'Dbs_Serial_Number': group['Dbs_Serial_Number'].iloc[-1],
                    'SMU': group['SMU'].iloc[-1],
                    'Smu_Date': group['Smu_Date'].iloc[-1],
                    'Source': group['Source'].iloc[-1],
                }

                valid_data = group[~group['is_suspicious']].copy()

                for window_name, days in windows.items():
                    window_start = reference_date - timedelta(days=days)
                    window_data = valid_data[valid_data['Smu_Date'].between(window_start, reference_date, inclusive='both')]
                    
                    if len(window_data) >= 2:
                        # Vectorized calculation with rate capping
                        smu_diff = window_data['SMU'].diff().fillna(0)  # Fill NaN with 0 for first diff
                        time_diff = window_data['Smu_Date'].diff().dt.total_seconds() / 86400  # Fractional days
                        time_diff = time_diff.fillna(0)  # Fill NaN for first row
                        daily_rate = smu_diff / time_diff.replace(0, np.inf)  # Avoid div by zero
                        capped_rate = daily_rate.clip(lower=0, upper=max_rate_per_day)  # Cap at 0 to 24
                        capped_diff = np.where(time_diff > 0, capped_rate * time_diff, 0)  # Only positive diffs
                        total_smu_diff = capped_diff.sum()  # Total usage in window
                        avg_daily_usage = total_smu_diff / days  # Average over full window
                        stats[f'{window_name}_Avg_Daily_Usage'] = min(max(0, avg_daily_usage), self.max_daily_hours)
                        
                        if window_name == '365D':
                            stats['Annual_Usage'] = max(0, total_smu_diff)  # Ensure non-negative
                    else:
                        stats[f'{window_name}_Avg_Daily_Usage'] = 0
                        if window_name == '365D':
                            stats['Annual_Usage'] = 0

                results.append(stats)

            results_df = pd.DataFrame(results)

            def compute_weighted_usage(row):
                valid_values = []
                valid_weights = []
                for w in ['10D', '30D', '180D', '365D']:
                    val = row.get(f'{w}_Avg_Daily_Usage')
                    if pd.notna(val) and val > 0:
                        valid_values.append(val)
                        valid_weights.append(self.weights[f'{w}_Avg_Daily_Usage'])
                return np.average(valid_values, weights=valid_weights) if valid_values else None

            results_df['Weighted_Avg_Usage'] = results_df.apply(compute_weighted_usage, axis=1)

            logging.info(f"Analysis complete: {len(results_df)} records processed")
            return results_df

        except Exception as e:
            logging.error(f"Error in analyze_usage: {str(e)}")
            raise