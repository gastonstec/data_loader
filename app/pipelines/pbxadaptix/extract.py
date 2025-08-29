# app/pipelines/pbxadaptix/extract.py
from loguru import logger
import duckdb

global db_pool, duckdb_conn


def load_csv(uri: str):
    logger.info(r"Loading PBX data from file: {}".format(uri))
    try:
        r = duckdb.read_csv(uri, header=True, sep=",")
        print(r)
        r2 = duckdb.execute("SHOW ALL TABLES;")
        print(r2.fetchall())
    except Exception as e:
        raise ValueError(e)


def start(db_pool, dbinput_files: list[str]):
    db_pool = db_pool
    try:
        for uri in input_files:
            load_csv(uri)
    except Exception as e:
        logger.error("Error loading PBX data from CSV file: {}".format(e))
        return None
