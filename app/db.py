from loguru import logger
from core.config import DBSettings
from core.database import DBConnectionPool


# Create database connection pool
def open_db_pool() -> DBConnectionPool:
    # Load database settings from the environment
    db_settings = DBSettings()

    # Validate the database settings
    if not db_settings.check_values():
        raise ValueError("Invalid database settings provided.")

    # Log the connection details
    logger.info(
        f"Connecting to database {db_settings.dbname} at "
        f"{db_settings.host}:{db_settings.port}"
    )

    # Create a connection pool instance
    db_pool = DBConnectionPool(
        user=db_settings.user,
        password=db_settings.password,
        host=db_settings.host,
        port=db_settings.port,
        dbname=db_settings.dbname,
        appname=db_settings.appname,
        min_size=db_settings.min_size,
        max_size=db_settings.max_size,
        timeout=DBSettings.timeout_conn
    )

    # Connect the pool
    try:
        # Connect to the database        
        logger.info("Creating database connection...")
        db_pool.connect()
        db_version = db_pool.test()
        # Log the successful connection
        logger.info(
            f"Database connection established successfully. "
            f"Version: {db_version}"
        )
    except Exception as e:
        raise RuntimeError(f"Database connection failed: {e}")
    return db_pool


# Function to close the connection pool
def close_db_pool(db_pool=None):
    if not db_pool:
        raise ValueError("No database connection pool provided to close.")
    try:
        db_pool.close(DBSettings.timeout_conn)
    except Exception as e:
        logger.error(f"Error closing the database connection: {e}")
