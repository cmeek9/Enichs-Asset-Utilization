from sqlalchemy import create_engine
import pandas as pd
from config import logging

class DataPuller:
    """
    A class for pulling data from a stored procedure into a pandas DataFrame.
    """

    def __init__(self, connection_str):
        """
        Initialize the DataPuller with a database connection string.
        """
        self.connection_str = connection_str
        self.engine = create_engine(connection_str)

    def pull_data_with_stored_proc(self, stored_proc):
        """
        Pulls data from a stored procedure into a pandas DataFrame.
        """
        try:
            # Call the stored procedure
            query = "EXEC dbo.sp_AssetUtilization_GetAllData"
        
            # Pull data in batches for large result sets
            chunks = pd.read_sql_query(query, self.engine, chunksize=100000)
            full_df = pd.concat(chunks, ignore_index=True)

            # Post-process DataFrame
            full_df['SMU'] = full_df['SMU'].astype(float)
            full_df.drop_duplicates(inplace=True)
            self.whitespace_remover(full_df)

            logging.info('Successfully executed stored proc.')

            return full_df
        except Exception as e:
            logging.error(str(e))
        
    def whitespace_remover(self, df):
        """
        Removes leading and trailing whitespace from all object-type columns in the DataFrame.
        """
        for col in df.select_dtypes(include=['object']):
            df[col] = df[col].str.strip()
        return df