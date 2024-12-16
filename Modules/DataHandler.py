from sqlalchemy import create_engine
import pandas as pd
from config import logging

class DataHandler:
    """
    A class for handling data operations with a SQL database.
    """

    def __init__(self, connection_str):
        """
        Initialize the DataHandler with a database connection string.
        """
        self.connection_str = connection_str
        self.engine = create_engine(connection_str, fast_executemany=True)

    def write_new_data_to_sql(self, df, table_name):
        """
        Write a pandas DataFrame to a SQL table, replacing it if it exists.
        """
        try:
            with self.engine.begin() as connection:
                df.to_sql(table_name, con=connection, schema='dbo', if_exists='replace', index=False)
                logging.info(f"Replaced table '{table_name}' with {len(df)} rows.")
        except Exception as e:
            logging.error(f"An error occurred while writing to the table '{table_name}': {str(e)}")

