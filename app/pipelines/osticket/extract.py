# app/pipelines/osticket/extract.py
from loguru import logger
import duckdb
from .pipelineinfo import PIPENAME


# Load OSticket data from CSV files
def load_csv_file(uri: str) -> str:
    try:
        table_name = PIPENAME + "_" + uri.split("/")[-1].split(".")[0]
        duckdb.sql(f"CREATE TABLE {table_name} AS SELECT * FROM '{uri}';")
        return table_name
    except Exception as e:
        raise ValueError(e)


# Start the pipeline
def start(pipeline_folder, db_pool, input_files) -> list[str]:
    try:
        table_list = []
        for uri in input_files:
            table_name = load_csv_file(uri=uri)
            table_list.append(table_name)
        return table_list
    except Exception as e:
        logger.error(
            "Error loading OSticket data from CSV file: {}".format(e)
        )
        return []
