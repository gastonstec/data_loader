from loguru import logger
from .pbxadaptix import pipeline as pbxadaptix_pipeline


def execute_pipelines(base_folder, db_pool, duckdb_conn):
    logger.info("Executing pipelines")
    pbxadaptix_pipeline.start_pipeline(
        db_pool=db_pool, base_folder=base_folder, duckdb_conn=duckdb_conn
    )
    return True


def start(db_pool, base_folder):
    execute_pipelines(db_pool, base_folder)
    return True
