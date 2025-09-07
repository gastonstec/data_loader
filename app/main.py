import os
import time
from loguru import logger
from core.config import AppSettings, EnvSettings
from core.database import DBConnectionPool
from dbpool import open_db_pool, close_db_pool
import duckdb
from pipelines.pipelines import start as pipelines_start


# Configure logger
if EnvSettings.env == "dev":
    logger.add(
        f"{AppSettings.log_folder}/{AppSettings.name}.log",
        rotation="10 MB",
        level="DEBUG"
    )
else:
    logger.add(
        f"{AppSettings.log_folder}/{AppSettings.name}.log",
        rotation="10 MB",
        level=AppSettings.log_level
    )


# Open database connection pool
db_pool: DBConnectionPool
try:
    db_pool = open_db_pool()
except Exception as e:
    logger.error(f"Error creating database connection pool: {e}")
    print(f"Error creating database connection pool: {e}")
    os._exit(1)


# Start program
def start_program() -> bool:
    logger.info(f"{AppSettings.name} started")
    return True


# Stop program
def stop_program() -> bool:
    # Close database connection pool
    try:
        close_db_pool(db_pool)
        duckdb.close()
        logger.info(f"{AppSettings.name} database connection pool closed")
    except Exception as e:
        logger.error(f"Error closing database connection pool: {e}")
        print(f"Error closing database connection pool: {e}")
        return False
    return True


# Main program loop
def main_program_loop():
    # Start application loop
    try:
        counter = 0
        while True:
            counter += 1
            logger.info(
                f"{AppSettings.name} is running - iteration {counter}"
            )
            # Your actual daemon work here
            # Simulate some work
            pipelines_start(
                base_folder=AppSettings.base_folder,
                db_pool=db_pool
            )
            time.sleep(AppSettings.sleep_interval)
    except KeyboardInterrupt:
        print(f"{AppSettings.name} received stop signal")
        logger.info(f"{AppSettings.name} received stop signal")
    except Exception as e:
        print(f"Unexpected error: {e}")
        logger.error(f"Unexpected error: {e}")
    # End application loop

    # Stop section
    try:
        # Close the database connection pool
        close_db_pool(db_pool)
        if not stop_program():
            logger.error(
                f"{AppSettings.name} encountered an error during shutdown"
            )
    except Exception as e:
        logger.error(f"Unexpected error during shutdown: {e}")
    finally:
        logger.info(f"{AppSettings.name} stopped")


# Main program entry point
def main():
    main_program_loop()


# Start the main program
main()
logger.info(f"{AppSettings.name} finished")
print(f"{AppSettings.name} finished")
