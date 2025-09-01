# app/core/utils.py
import glob
import os


def list_csv_files(folder_path):
    """
    Lists all CSV files within a specified folder.

    Args:
        folder_path (str): The path to the folder to search.

    Returns:
        list: A list of full paths to the CSV files found.
    """
    # Construct the pattern to match all .csv files within the folder
    csv_pattern = os.path.join(folder_path, "*.csv")

    # Use glob.glob to find all files matching the pattern
    csv_files = glob.glob(csv_pattern)

    return csv_files
