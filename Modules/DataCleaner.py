import numpy as np
from config import logging

class DataCleaner:
    """
    A class for cleaning and normalizing SMU data.
    """

    
    def calculate_differences(self, df):
        """Calculate SMU and Days differences."""
        df = df.copy()
        df['SMU_Diff'] = df['SMU'].diff().where(df['Dbs_Serial_Number'].eq(df['Dbs_Serial_Number'].shift()))
        df['Days_Diff'] = df['Smu_Date'].diff().where(df['Dbs_Serial_Number'].eq(df['Dbs_Serial_Number'].shift())).dt.days
        return df

    
    def normalize_smu_differences(self, df):
        """Normalize SMU differences to get Daily_SMU."""
        df = df.copy()
        df['Daily_SMU'] = df['SMU_Diff'] / df['Days_Diff']
        df.loc[df['Daily_SMU'].isin([np.inf, -np.inf]), 'Daily_SMU'] = np.nan
        df.loc[df['Daily_SMU'].isna(), 'Daily_SMU'] = df['SMU_Diff']
        return df
    
    def interpolate_daily_smu(self, df):
        """Interpolate missing Daily_SMU values."""

        df = df.sort_values(['Dbs_Serial_Number', 'Smu_Date'])

        # Interpolate within each Dbs_Serial_Number group
        df['Daily_SMU'] = df.groupby('Dbs_Serial_Number')['Daily_SMU'].transform(
            lambda x: x.interpolate(limit=10, limit_direction='forward')
        )
        return df

    
    def filter_valid_rows(self, df):
        """Filter rows where Daily_SMU is between 0 and 24."""
        valid_diff = (df['Daily_SMU'] >= 0) & (df['Daily_SMU'] <= 24)
        return df[valid_diff]

    
    def add_min_smu_date(self, df, min_dates):
        """Add the true minimum Smu_Date to the DataFrame."""
        return df.merge(min_dates, left_on='Dbs_Serial_Number', right_index=True, how='left')

    
    def filter_smu_range(self, df):
        """Filter data by SMU range."""
        return df[df['SMU'].between(1, 200000)]

    
    def sort_dataframe(self, df):
        """Sort the DataFrame to ensure chronological order within each Serial Number."""
        return df.sort_values(by=['Dbs_Serial_Number', 'Smu_Date'], ascending= True)

    
    def get_min_smu_dates(self, df):
        """Get the true minimum Smu_Date for each Dbs_Serial_Number."""
        min_dates = df.groupby('Dbs_Serial_Number', sort=False)['Smu_Date'].min()
        min_dates.name = 'Min_Smu_Date'
        return min_dates

    
    def keep_latest_smu_date(self, df):
        """Keep the latest Smu_Date for each Dbs_Serial_Number and SMU."""
        idx_max_smu = df.groupby(['Dbs_Serial_Number', 'SMU'], sort=False)['Smu_Date'].idxmax()
        return df.loc[idx_max_smu].reset_index(drop=True)

    
    def keep_highest_smu_with_priority(self, df):
        """Keep the highest SMU, prioritizing Source, for each Dbs_Serial_Number and Smu_Date."""
        # Assign priority ranks to the Source column
        priority_map = {'Bd': 1, 'W': 2, 'So': 3}
        df['Source_Priority'] = df['Source'].map(priority_map).fillna(float('inf'))  # Handle cases where Source isn't in the map

        # Sort by SMU (descending), Source priority (ascending), and Smu_Date (descending)
        sorted_df = df.sort_values(
            by=['Dbs_Serial_Number', 'Smu_Date', 'SMU', 'Source_Priority'],
            ascending=[True, True, False, True]
        )

        # Drop duplicates to keep the row with the highest SMU for each group
        result = sorted_df.drop_duplicates(subset=['Dbs_Serial_Number', 'Smu_Date'], keep='first').reset_index(drop=True)

        # Drop the temporary Source_Priority column
        result = result.drop(columns=['Source_Priority'], errors='ignore')

        return result


    
    def process_and_clean_data(self, full_df):
        """Process and clean the data."""
        try:
            full_df = DataCleaner.filter_smu_range(self, full_df)
            logging.info('SMU ranged fitlered.')
            full_df = DataCleaner.sort_dataframe(self, full_df)
            logging.info('df sorted.')
            min_dates = DataCleaner.get_min_smu_dates(self, full_df)
            logging.info('Min SMU dates acquired.')
            full_df = DataCleaner.keep_latest_smu_date(self, full_df)
            logging.info('keeping latest SMU date.')
            full_df = DataCleaner.keep_highest_smu_with_priority(self, full_df)
            logging.info('keeping latest SMU on priority.')
            full_df = DataCleaner.calculate_differences(self, full_df)
            logging.info('calculating SMU differences.')
            full_df = DataCleaner.normalize_smu_differences(self, full_df)
            logging.info('normalizing SMU differences.')
            full_df = DataCleaner.interpolate_daily_smu(self, full_df)
            logging.info('Interpolated missing Daily_SMU values.')
            cleaned_df = DataCleaner.filter_valid_rows(self, full_df)
            logging.info('filtering SMU rows.')
            cleaned_df = DataCleaner.add_min_smu_date(self, cleaned_df, min_dates)
            logging.info('Min SMU date added.')
            logging.info('Data cleaned and processed successfully.')
            return cleaned_df
        except Exception as e:
            logging.error(str(e))



# full_df = DataCleaner.remove_or_correct_anomalies(self, full_df)
# between interoplate data and filter valid rows if you put this back in the orchestrator
# logging.info('Removing or correcting anomalies with IQR.')

    # def remove_or_correct_anomalies(self, df):
    #     """Using IQR stats technique to remove ."""
        
    #     df.loc[(df['Daily_SMU'] > 24) | (df['Daily_SMU'] < 0), 'Daily_SMU'] = np.nan
        
    #     # Outlier detection using IQR:
    #     grouped = df.groupby('Dbs_Serial_Number')
    #     def iqr_outliers(group):
    #         q1 = group['Daily_SMU'].quantile(0.25)
    #         q3 = group['Daily_SMU'].quantile(0.75)
    #         iqr = q3 - q1
    #         upper_bound = q3 + 0.95 * iqr
    #         lower_bound = q1 - 0.95 * iqr
    #         # Set outliers to NaN
    #         group.loc[(group['Daily_SMU'] > upper_bound) | (group['Daily_SMU'] < lower_bound), 'Daily_SMU'] = np.nan
    #         return group

    #     df = grouped.apply(iqr_outliers).reset_index(drop=True)
    #     return df