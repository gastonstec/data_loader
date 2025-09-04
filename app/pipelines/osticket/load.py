
from loguru import logger
import duckdb
from .pipelineinfo import (
    PIPENAME,
    PROCESSED_PREFIX
)


def write_csv_file(table_name: str, pipeline_folder: str):
    try:
        duckdb.sql(
            f"COPY {table_name} TO "
            f"'{pipeline_folder}/{PROCESSED_PREFIX}{table_name}.csv' "
            f"(HEADER, DELIMITER ',');"
        )
    except Exception as e:
        logger.error(
            f"Error loading {PIPENAME} data from table {table_name}: {e}"
        )


def start(pipeline_folder, db_pool, input_files, table_list: list[str]):
    for table in table_list:
        write_csv_file(table, pipeline_folder)
