# app/pipelines/pbxadaptix/pipeline.py
import os
from loguru import logger
from .extract import start as extract_start
# from .transform import start as transform_start
from core.utils import list_csv_files


PIPENAME = "gtimpbxadaptix"


def create_folder_structure(pipeline_folder: str):
    try:
        os.makedirs(pipeline_folder, exist_ok=True)
    except Exception as e:
        logger.error(f"Error creating folder structure: {e}")


def read_input_files(pipeline_folder: str) -> list[str]:
    logger.info(f"Reading files from folder: {pipeline_folder}")
    files = list_csv_files(pipeline_folder)
    return files


def start(base_folder, db_pool, duckdb_conn):
    logger.info("Starting GTIM PBX Adaptix pipeline")
    # Create folder structure
    pipeline_folder = f"{base_folder}/{PIPENAME}"
    create_folder_structure(pipeline_folder)
    # Read input files
    input_files = read_input_files(pipeline_folder)

    # Extract data
    extract_start(
        base_folder=base_folder,
        db_pool=db_pool,
        input_files=input_files,
        duckdb_conn=duckdb_conn
    )

    result = duckdb_conn.execute("SHOW TABLES;").fetchall()
    print(result)

    # Transform data

    # Load data

    return True
