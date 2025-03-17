from Modules.DataPuller import DataPuller
from Modules.DataHandler import DataHandler
from Modules.EquipmentUsageAnalyzer import EquipmentUsageAnalyzer
from config import logging
import time

def main():
    logging.info(f"Starting the program.")

    # ETL process steps
    logging.info(f"Starting extraction process for data.")

    data_puller = DataPuller()

    # Extraction
    start_time = time.time()
    full_df = data_puller.pull_data_with_stored_proc()
    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"extraction took {elapsed_time:.2f} seconds.")

    logging.info(f"Starting transformation process for data.")
    
    # Initialize new analyzer
    analyzer = EquipmentUsageAnalyzer()

    # Analyze usage (cleaning and transforming steps)
    start_time = time.time()
    results_df = analyzer.analyze_usage(full_df)
    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"transformation took {elapsed_time:.2f} seconds.")

    logging.info(f"Starting Load process for data.")

    # Load
    data_handler = DataHandler()  # Removed load_conxn_str
    data_handler.write_new_data_to_sql(results_df, 'Prod_AssetUtilization')

    logging.info(f"Asset Utilization Complete.")

if __name__ == '__main__':
    main()
