from loguru import logger
import pandas as pd
from psycopg_pool import ConnectionPool
import uuid
from models import PBXAdaptixCall

# Service for loading PBX calls data from CSV files into a PostgreSQL database
class PBXAdaptixService:

    # Load csv file with PBX calls
    def pbx_calls_load_csv(self, uri:str):
        logger.info(r"Loading PBX data from file: {}".format(uri))
        try:
            df = pd.read_csv(filepath_or_buffer=uri, encoding='utf-8', low_memory=False)
        except Exception as e:
            raise ValueError(e)
        return df
    
    def pbx_calls_load(self, pool:ConnectionPool, process_id: str, uri:str):
        # Load PBX data from CSV file
        try:
            df = self.pbx_calls_load_csv(uri)
        except Exception as e:
            logger.error("Error loading PBX data from CSV file: {}".format(e))
            raise ValueError(e)
        
        # Calculate the number of rows in the DataFrame
        num_rows = len(df)
        logger.info("Number of rows to be loaded: {}".format(num_rows))
        try:
            pbxModel = PBXAdaptixCall(pool=pool)
            pbxModel.insert_bulk(df=df, process_id=process_id)
        except Exception as e:
            logger.error("Error getting connection from pool: {}".format(e))
            raise ValueError(e)
        logger.info("Connection obtained from pool")

        # Return processed DataFrame
        del df
        print("Number of rows loaded: {}".format(num_rows))
        return num_rows
