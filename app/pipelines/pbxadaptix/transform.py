# app/pipelines/pbxadaptix/transform.py
from loguru import logger
import duckdb


def transform(input_files: list[str]):
    try:
        logger.info(r"Transforming PBX data")
        # Perform transformation logic here
        for file in input_files:
            duckdb.execute(f"ALTER TABLE '{file}' DROP COLUMN column9;")
    except Exception as e:
        logger.error(f"Error transforming PBX data: {e}")


def start(db_pool, input_files: list[str]):
    logger.info("Starting transformation")
    transform(input_files)
