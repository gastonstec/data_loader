# app/pipelines/pbxadaptix/extract.py
from loguru import logger


# Load PBX data from CSV files
def load_csv_file(uri: str, duckdb_conn):
    logger.info(r"Loading PBX data from file: {}".format(uri))
    try:
        # duckdb_conn.read_csv(uri, header=True, sep=",")
        duckdb_conn.execute(
            "CREATE TABLE IF NOT EXISTS pbx_calls AS ",
            f"SELECT * FROM read_csv_auto('{uri}');"
        )
    except Exception as e:
        raise ValueError(e)


# Start the pipeline
def start(base_folder, db_pool, duckdb_conn, input_files):
    try:
        for uri in input_files:
            load_csv_file(uri=uri, duckdb_conn=duckdb_conn)
            r2 = duckdb_conn.execute("SHOW ALL TABLES;")
            print(r2.fetchall())
    except Exception as e:
        logger.error("Error loading PBX data from CSV file: {}".format(e))
        return None
