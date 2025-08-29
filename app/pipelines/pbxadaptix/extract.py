from loguru import logger
import pandas as pd


# Load csv file with PBX calls
# uri = "path/to/your/csvfile.csv"
def pbx_calls_load_csv(uri: str):
    logger.info(r"Loading PBX data from file: {}".format(uri))
    try:
        df = pd.read_csv(
            filepath_or_buffer=uri,
            encoding='utf-8',
            low_memory=False
        )
    except Exception as e:
        raise ValueError(e)
    return df


def start(uri: str):
    try:
        df = pbx_calls_load_csv(uri)
    except Exception as e:
        logger.error("Error loading PBX data from CSV file: {}".format(e))
        return None
    # Do something with the DataFrame
    return df
