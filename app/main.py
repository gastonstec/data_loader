import os
import time
from loguru import logger
from core.config import AppSettings, EnvSettings
from db import open_db_pool, close_db_pool

# Configure logger
if not EnvSettings.env == "dev":
    logger.add(
        f"{AppSettings.app_folder}/{AppSettings.name}.log",
        rotation="10 MB"
    )

# Create database connection pool
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
        logger.info(f"{AppSettings.name} database connection pool closed")
    except Exception as e:
        logger.error(f"Error closing database connection pool: {e}")
        print(f"Error closing database connection pool: {e}")
        return False
    return True


# Main program loop
def main_program():
    # Start section
    if not start_program():
        logger.error(f"{AppSettings.name} failed to start")
        return

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
            print("Simulating work...")
            time.sleep(2)
    except KeyboardInterrupt:
        print(f"{AppSettings.name} received stop signal")
        logger.info(f"{AppSettings.name} received stop signal")
    except Exception as e:
        print(f"Unexpected error: {e}")
        logger.error(f"Unexpected error: {e}")

    # Stop section
    try:
        # Close the database connection pool
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
    main_program()


# Start the main program
main()
logger.info(f"{AppSettings.name} finished")
print(f"{AppSettings.name} finished")
