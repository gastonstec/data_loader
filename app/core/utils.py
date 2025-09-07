# app/core/utils.py
import glob
import os
from .config import AppSettings


# Lists all CSV files within a specified folder.
def list_csv_files(folder_path: str) -> list[str]:
    # Construct the pattern to match all .csv files within the folder
    csv_pattern = os.path.join(folder_path, "*.csv")

    # Use glob.glob to find all files matching the pattern
    csv_files = glob.glob(csv_pattern)

    return csv_files


# Creates a folder if it doesn't already exist.
def create_folder(folder_path: str) -> bool:
    try:
        os.makedirs(folder_path, exist_ok=True)
    except Exception as e:
        raise ValueError(f"Error creating folder structure: {e}")
    return True


# Creates the necessary folder structure for a pipeline.
def create_pipeline_folders(base_folder: str, pipename: str) -> bool:
    # Create base folder
    pipeline_folder = f"{base_folder}/{pipename}"
    create_folder(pipeline_folder)

    # Create processed subfolder
    processed_folder = f"{pipeline_folder}/{AppSettings.processed_folder}"
    create_folder(processed_folder)

    return True


# Lists input files from the pipeline folder.
def list_input_files(pipeline_folder: str) -> list[str]:
    files = list_csv_files(pipeline_folder)
    return files
