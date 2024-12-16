from Modules.DataPuller import DataPuller
from Modules.DataAnalyzer import DataAnalyzer
from Modules.DataCleaner import DataCleaner
from Modules.DataHandler import DataHandler
from config import *
import time

def main():

    print(f"Starting the program.")

    # all ETL processes are called as steps with variables housing data unil upload
    print()
    print(f"Starting extraction process for data.")
    print()

    
    data_puller = DataPuller(pull_conxn_str)

    # Extraction
    start_time = time.time()
    full_df = data_puller.pull_data_with_stored_proc(pull_conxn_str) 
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"extraction took {elapsed_time:.2f} seconds.")

    print()
    print(f"Starting transformation process for data.")
    print()
    
    # Transform
    data_cleaner = DataCleaner()

    start_time = time.time()
    cleaned_df = data_cleaner.process_and_clean_data(full_df) # in main

    # generic time frames needed for average SMU hours inbetween these
    days_list = [10, 30, 90, 180, 365]

    data_analyzer = DataAnalyzer

    average_usage_df = data_analyzer.calculate_average_hours_per_day(cleaned_df, days_list)

    smu_idx = average_usage_df.groupby(['Dbs_Serial_Number'])['SMU'].idxmax()
    average_usage_df = average_usage_df.loc[smu_idx].reset_index(drop=True)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"transformation took {elapsed_time:.2f} seconds.")
    

    print()
    print(f"Starting Load process for data.")
    print()

    # Load
    data_handler = DataHandler(load_conxn_str)
    data_handler.write_new_data_to_sql(average_usage_df, 'CMtest_assetUtilization')


    print(f"DONE.")



#  standard way of calling main, tells where to kick the python project from aka entry point
if __name__ == '__main__':
    main()