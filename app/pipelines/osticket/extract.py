# app/pipelines/osticket/extract.py
from loguru import logger
import duckdb
from .pipelineinfo import PIPENAME


# Create duckdb table from CSV file
def load_csv_file(uri: str) -> str:
    try:
        # Create table name from file name
        table_name = PIPENAME + "_" + uri.split("/")[-1].split(".")[0]
        # Create table in duckdb
        duckdb.sql(f"CREATE TABLE {table_name} AS SELECT * FROM '{uri}';")
        # Return the created table name
        return table_name
    except Exception as e:
        raise ValueError(e)


# Check if the file is valid for processing
def check_csv_file(table_name: str) -> bool:
    # Placeholder for file validation logic
    return True


# Start the pipeline
def start(pipeline_folder, db_pool, input_files) -> list[str]:
    # Create table list
    table_list = list[str]()
    for uri in input_files:
        try:
            # Load each CSV file into a duckdb table
            table_name = load_csv_file(uri=uri)
            table_list.append(table_name)
        except Exception as e:
            logger.error(
                f"Error loading {PIPENAME} data from CSV file {uri}: {e}"
            )
    return table_list
