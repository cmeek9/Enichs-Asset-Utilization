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
        """Process data in chunks for better performance."""
        try:
            chunk = chunk.sort_values(['Serial_Number', 'Smu_Date'])
            
            # Calculate previous values and differences
            chunk['prev_date'] = chunk.groupby('Serial_Number')['Smu_Date'].shift()
            chunk['prev_smu'] = chunk.groupby('Serial_Number')['SMU'].shift()
            
            chunk['time_diff'] = (chunk['Smu_Date'] - chunk['prev_date']).dt.total_seconds() / 86400
            chunk['smu_diff'] = chunk['SMU'] - chunk['prev_smu']
            
            # Identify suspicious readings
            daily_rate = chunk['smu_diff'] / chunk['time_diff']
            chunk['is_suspicious'] = (
                (daily_rate > self.max_daily_hours) |
                (chunk['smu_diff'] < 0) |
                (daily_rate.isna()) |
                (daily_rate == 0)
            )
            
            return chunk[['Serial_Number', 'Dbs_Serial_Number', 'SMU', 'Smu_Date', 'Source', 
                         'smu_diff', 'time_diff', 'is_suspicious']]
        except Exception as e:
            logging.error(f"Error during preprocessing chunk: {str(e)}")
            raise

    def analyze_usage(self, df, reference_date=None, chunk_size=1000):
        """Analyze usage over multiple time windows + annual usage."""
        try:
            if reference_date is None:
                reference_date = datetime.now()
                
            logging.info(f"Starting usage analysis with {len(df)} initial rows")
            
            # Remove duplicates
            df = df.drop_duplicates(['Serial_Number', 'Smu_Date', 'SMU'])
            logging.info(f"Removed duplicates; processing {len(df)} rows")
            
            # Process chunks
            chunks = [g for _, g in df.groupby(np.arange(len(df)) // chunk_size)]
            processed_df = pd.concat([self.preprocess_chunk(chunk) for chunk in chunks], 
                                ignore_index=True)
            
            # Initialize analysis
            results = []
            windows = {'10D': 10, '30D': 30, '90D': 90, '180D': 180, '365D': 365}
            
            # Analyze each serial number
            for serial, group in processed_df.groupby('Serial_Number'):
                if len(group) < 2:
                    continue
                    
                stats = {
                    'Serial_Number': serial,
                    'Dbs_Serial_Number': group['Dbs_Serial_Number'].iloc[0],
                    'SMU': group['SMU'].iloc[-1],
                    'Smu_Date': group['Smu_Date'].iloc[-1],
                    'Source': group['Source'].iloc[-1],
                }
                
                valid_data = group[~group['is_suspicious']]
                
                # Calculate window averages
                for window_name, days in windows.items():
                    window_start = reference_date - timedelta(days=days)
                    window_data = valid_data[valid_data['Smu_Date'] >= window_start]
                    
                    if len(window_data) >= 2:
                        total_smu_diff = window_data['smu_diff'].sum()
                        avg_daily_usage = min(total_smu_diff / days, self.max_daily_hours)
                        stats[f'{window_name}_Avg_Daily_Usage'] = avg_daily_usage
                    else:
                        stats[f'{window_name}_Avg_Daily_Usage'] = None
                
                # Calculate yearly average
                valid_data = valid_data.sort_values('Smu_Date')
                total_smu_diff = valid_data['smu_diff'].sum()
                total_time_diff = valid_data['time_diff'].sum()
                
                if total_time_diff > 0:
                    yearly_usage = (total_smu_diff / total_time_diff) * 365
                    stats['Avg_Yearly_Usage'] = max(0, min(yearly_usage, self.max_daily_hours * 365))
                else:
                    stats['Avg_Yearly_Usage'] = None
                    
                results.append(stats)
            
            results_df = pd.DataFrame(results)
            
            def compute_weighted_usage(row):
                valid_values, valid_weights = [], []
                for w in ['10D', '30D', '180D', '365D']:
                    val = row[f'{w}_Avg_Daily_Usage']
                    if pd.notna(val):
                        valid_values.append(val)
                        valid_weights.append(self.weights[f'{w}_Avg_Daily_Usage'])

                return np.average(valid_values, weights=valid_weights) if valid_values else None

            results_df['Weighted_Avg_Usage'] = results_df.apply(compute_weighted_usage, axis=1)
            
            logging.info(f"Analysis complete. Processed {len(results_df)} records.")
            return results_df
            
        except Exception as e:
            logging.error(f"Error during usage analysis: {str(e)}")
            raise