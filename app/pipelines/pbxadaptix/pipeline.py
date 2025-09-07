# app/pipelines/pbxadaptix/pipeline.py
from loguru import logger
from .extract import start as extract_start
# from .transform import start as transform_start
from core.utils import create_pipeline_folders, list_input_files 


PIPENAME = "gtimpbxadaptix"


def start(base_folder, db_pool):
    # Log start of pipeline
    logger.info("Starting GTIM PBX Adaptix pipeline")

    # Create pipeline folder
    if not create_pipeline_folders(base_folder, PIPENAME):
        raise ValueError("Error creating pipeline folder structure")

    # Define pipeline folder
    pipeline_folder = f"{base_folder}/{PIPENAME}"

    # Read input files
    input_files = list_input_files(pipeline_folder)

    if len(input_files) == 0:
        return

    """ EXTRACT """
    # Call the extract function
    extract_start(
        base_folder=base_folder,
        db_pool=db_pool,
        input_files=input_files,
    )

    """ TRANSFORM """

    """ LOAD """

    """ FINAL TASKS """

    # Log end of pipeline
    logger.info("GTIM PBX Adaptix pipeline finished successfully")

    return True
