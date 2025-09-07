from loguru import logger
from .pbxadaptix import pipeline as pbxadaptix_pipeline
# from .osticket import pipeline as osticket_pipeline

# List of all pipelines
pipelines_list = [
    pbxadaptix_pipeline
]


# Function to execute all pipelines
def execute_pipelines(base_folder, db_pool):
    logger.info("Start pipelines")
    try:
        for pipeline in pipelines_list:
            pipeline.start(
                base_folder=base_folder,
                db_pool=db_pool
            )
    except Exception as e:
        logger.error(f"Error executing pipelines: {e}")


# Start the pipeline
def start(base_folder, db_pool):
    try:
        execute_pipelines(base_folder=base_folder, db_pool=db_pool)
    except Exception as e:
        logger.error(f"Error starting pipelines: {e}")
