# app/pipelines/osticket/pipeline.py
import os
import duckdb
from loguru import logger

from .extract import start as extract_start
from .transform import start as transform_start
from .load import start as load_start
from core.utils import list_csv_files
from .pipelineinfo import PIPENAME


# Create pipeline folder structure
def create_folder_structure(pipeline_folder: str):
    try:
        os.makedirs(pipeline_folder, exist_ok=True)
    except Exception as e:
        logger.error(
            f"Error creating folder structure for {pipeline_folder}: {e}"
        )


# Read input files from pipeline folder
def read_input_files(pipeline_folder: str) -> list[str]:
    # Log file reading
    logger.info(f"Reading files from folder: {pipeline_folder}")

    # List CSV files from pipeline folder
    try:
        file_list = list_csv_files(pipeline_folder)
        return file_list
    except Exception as e:
        logger.error(f"Error reading input files from {pipeline_folder}: {e}")
        return []


def drop_tables(table_list: list[str]):
    for table in table_list:
        duckdb.execute(f"DROP TABLE IF EXISTS {table};")


""" def rename_processed_files(input_files: list[str]) -> list[str]:
    renamed_files = []
    for file in input_files:
        new_name = file + PROCESSED_SUFFIX
        os.rename(file, new_name)
        renamed_files.append(new_name)
    return renamed_files """


# Start the pipeline
def start(base_folder, db_pool) -> bool:
    # Log pipeline start
    logger.info(f"Starting {PIPENAME} pipeline")

    # Create folder structure
    pipeline_folder = f"{base_folder}/{PIPENAME}"
    create_folder_structure(pipeline_folder)

    # Read input files
    input_files = read_input_files(pipeline_folder)

    """ Extract data section """
    # Start extraction
    table_list = list[str]()
    try:
        table_list = extract_start(
            pipeline_folder=pipeline_folder,
            db_pool=db_pool,
            input_files=input_files,
        )
    except Exception as e:
        logger.error(f"Error extracting {PIPENAME} data: {e}")
        return False

    """ Transform data section """
    # Start transformation
    try:
        transform_start(
            pipeline_folder=pipeline_folder,
            db_pool=db_pool,
            input_files=input_files,
            table_list=table_list,
        )
    except Exception as e:
        logger.error(f"Error transforming {PIPENAME} data: {e}")
        return False

    """ Load data section """
    # Start loading
    try:
        load_start(
            pipeline_folder=pipeline_folder,
            db_pool=db_pool,
            input_files=input_files,
            table_list=table_list,
        )
        print(f"Loading {PIPENAME} data to destination")
    except Exception as e:
        logger.error(f"Error loading {PIPENAME} data: {e}")
        return False

    """ Finally section """
    # Rename processed files
    # try:
    #     rename_processed_files(input_files)
    # except Exception as e:
    #     logger.error(
    #         f"Error renaming processed files for {PIPENAME} pipeline: {e}"
    #     )
    #     return False

    # Drop duckdb tables
    try:
        drop_tables(table_list)
    except Exception as e:
        logger.error(f"Error dropping tables for {PIPENAME} pipeline: {e}")
        return False

    return True
