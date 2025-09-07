# app/pipelines/pbxadaptix/extract.py
from loguru import logger
import duckdb


# Load PBX data from CSV files
def load_csv_file(uri: str):
    logger.info(r"Loading PBX data from file: {}".format(uri))
    try:
        r = duckdb.read_csv(uri, header=True)
        print(r)
    except Exception as e:
        raise ValueError(e)


# Start the pipeline
def start(base_folder, db_pool, input_files):
    try:
        for uri in input_files:
            load_csv_file(uri=uri)
    except Exception as e:
        logger.error("Error loading PBX data from CSV file: {}".format(e))
        return None
