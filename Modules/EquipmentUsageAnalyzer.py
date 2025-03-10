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

    def preprocess_data(self, df):
        """
        Preprocesses the entire dataframe:
          - Groups by (Serial_Number, Smu_Date) to aggregate values.
          - Sorts by Serial_Number and Smu_Date.
          - Computes differences (smu_diff, time_diff) and flags suspicious data.
        """
        try:
            processed_df = df.groupby(['Serial_Number', 'Smu_Date']).agg({
                'SMU': 'max',
                'Dbs_Serial_Number': 'last',
                'Source': 'last'
            }).reset_index()

            processed_df = processed_df.sort_values(['Serial_Number', 'Smu_Date'])
            processed_df['prev_date'] = processed_df.groupby('Serial_Number')['Smu_Date'].shift(1)
            processed_df['prev_smu'] = processed_df.groupby('Serial_Number')['SMU'].shift(1)

            processed_df['time_diff'] = (processed_df['Smu_Date'] - processed_df['prev_date']).dt.total_seconds() / 86400
            processed_df['smu_diff'] = processed_df['SMU'] - processed_df['prev_smu']
            processed_df['is_suspicious'] = processed_df['smu_diff'] < 0

            return processed_df[['Serial_Number', 'Dbs_Serial_Number', 'SMU', 'Smu_Date', 'Source', 
                                'smu_diff', 'time_diff', 'is_suspicious']]
        except Exception as e:
            logging.error(f"Error in preprocess_data: {str(e)}")
            raise

    def analyze_usage(self, df, reference_date=None):
        """
        Orchestrator:
          1) Deduplicates the data.
          2) Preprocesses the data.
          3) Analyzes each Serial_Number group.
          4) Computes Weighted_Avg_Usage.
        """
        if reference_date is None:
            reference_date = datetime.now()
        logging.info(f"Starting analysis with {len(df)} rows, reference_date={reference_date}")

        # Deduplication
        df = df.drop_duplicates(['Serial_Number', 'Smu_Date', 'SMU'])
        logging.info(f"After deduplication: {len(df)} rows")

        processed_df = self.preprocess_data(df)
        logging.info("Preprocessing complete.")

        windows = {'10D': 10, '30D': 30, '90D': 90, '180D': 180, '365D': 365}
        
        # Create window start dates once
        window_start_dates = {
            window_name: reference_date - timedelta(days=days) 
            for window_name, days in windows.items()
        }
        
        # Pre-filter data based on suspicion flag to avoid repeated filtering
        valid_data_df = processed_df[~processed_df['is_suspicious']].copy()
        
        # Add window flags to each row for faster filtering later
        for window_name, start_date in window_start_dates.items():
            valid_data_df[f'in_{window_name}'] = valid_data_df['Smu_Date'].between(
                start_date, reference_date, inclusive='both'
            )
        
        results = []
        
        # For each serial, get the latest entry info once
        latest_info = processed_df.sort_values('Smu_Date').groupby('Serial_Number').last().reset_index()
        latest_info_dict = {
            row['Serial_Number']: {
                'Dbs_Serial_Number': row['Dbs_Serial_Number'],
                'SMU': row['SMU'],
                'Smu_Date': row['Smu_Date'],
                'Source': row['Source']
            } for _, row in latest_info.iterrows()
        }
        
        # Group data by serial for processing
        for serial, group in valid_data_df.groupby('Serial_Number'):
            try:
                if len(group) < 1:
                    continue
                
                # Get the latest info for this serial
                latest = latest_info_dict.get(serial)
                if not latest:
                    continue
                
                stats = {
                    'Serial_Number': serial,
                    'Dbs_Serial_Number': latest['Dbs_Serial_Number'],
                    'SMU': latest['SMU'],
                    'Smu_Date': latest['Smu_Date'],
                    'Source': latest['Source']
                }
                
                max_rate_per_day = 24 # setting hard caps for averages per window
                
                # Process each time window once
                for window_name, days in windows.items():
                    # Use pre-computed window flags to filter data
                    window_data = group[group[f'in_{window_name}']]
                    
                    if len(window_data) >= 2:
                        # Sort window data once before calculations
                        window_data = window_data.sort_values('Smu_Date')
                        
                        # Pre-compute diffs in one operation
                        smu_diff = window_data['SMU'].diff().fillna(0)
                        time_diff_days = window_data['Smu_Date'].diff().dt.total_seconds() / 86400
                        time_diff_days = time_diff_days.fillna(0)
                        
                        # Optimize calculations by using NumPy operations directly
                        daily_rate = np.divide(
                            smu_diff.values, 
                            np.where(time_diff_days.values == 0, np.inf, time_diff_days.values)
                        )
                        capped_rate = np.clip(daily_rate, 0, max_rate_per_day)

                        time_diff_mask = time_diff_days.values > 0
                        capped_diff = np.zeros_like(capped_rate)
                        capped_diff[time_diff_mask] = capped_rate[time_diff_mask] * time_diff_days.values[time_diff_mask]
                        
                        total_smu_diff = np.sum(capped_diff)
                        avg_daily_usage = total_smu_diff / days
                        
                        stats[f'{window_name}_Avg_Daily_Usage'] = min(
                            max(0, avg_daily_usage),
                            self.max_daily_hours
                        )
                        
                        if window_name == '365D':
                            stats['Annual_Usage'] = max(0, total_smu_diff)
                    else:
                        stats[f'{window_name}_Avg_Daily_Usage'] = 0
                        if window_name == '365D':
                            stats['Annual_Usage'] = 0
                
                results.append(stats)
            except Exception as e:
                logging.error(f"Error analyzing serial {serial}: {str(e)}")
                continue
        
        results_df = pd.DataFrame(results)
        logging.info(f"Serial group analysis complete: analyzed {len(results)} groups.")
        
        if not results_df.empty:
            results_df['Weighted_Avg_Usage'] = results_df.apply(
                lambda row: self.compute_weighted_usage(row), axis=1
            )
        
        logging.info(f"Analysis complete: {len(results_df)} records processed.")
        return results_df
    
    def compute_weighted_usage(self, row):
        """Helper method to compute weighted average usage"""
        valid_values = []
        valid_weights = []
        for w in ['10D', '30D', '180D', '365D']:
            val = row.get(f'{w}_Avg_Daily_Usage', 0)
            if pd.notna(val) and val > 0:
                valid_values.append(val)
                valid_weights.append(self.weights[f'{w}_Avg_Daily_Usage'])
        return np.average(valid_values, weights=valid_weights) if valid_values else None