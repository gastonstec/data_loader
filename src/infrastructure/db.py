from psycopg import Connection
from psycopg_pool import ConnectionPool
import logging

logger = logging.getLogger(__name__)

            
class DBPool:

    # Class init
    def __init__(self, user: str, password: str, \
                 host: str, port: int, dbname: str, appname: str):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname
        self.appname = appname
    
    # Create a connection pool
    def connect(self, min_size: int, max_size: int, timeout: float):
        # Build connection string
        connstr =  f"user={self.user} " \
            f"password={self.password} " \
            f"host={self.host} " \
            f"port={self.port} " \
            f"dbname={self.dbname} " \
            f"application_name='{self.appname}'"
        
        # Check pool settings
        if min_size < 0 or max_size < 0 or timeout < 0:
            raise ValueError("Pool settings must be non-negative")
        if min_size > max_size:
            raise ValueError("Minimum pool size must be less than or equal to maximum pool size")
        if min_size > 100 or max_size > 100:
            raise ValueError("Pool settings must be less than or equal to 100")
        if timeout > 60:
            raise ValueError("Timeout must be less than or equal to 60 seconds")
        if min_size < 1:
            raise ValueError("Minimum pool size must be greater than or equal to 1")
        
        # Create pool 
        pool = ConnectionPool(conninfo=connstr, min_size=min_size, max_size=max_size, timeout=timeout)

        # Check connection
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version()")
                    result = cur.fetchone()
                    print(f"Pool connected to PostgreSQL version: {result}")

            # Commit changes (if any)
            conn.commit()
        except Exception as e:
            #logger.error(f"Error connecting pool to PostgreSQL: {e}")
            conn.rollback() # Rollback in case of error
            raise ValueError(e)
        
        # Return ConnectionPool
        return pool
    

    # Close pool
    def close(pool: ConnectionPool):
        try:
            pool.close()
            pool.wait()
        except Exception as e:
            raise ValueError(e)
        return
