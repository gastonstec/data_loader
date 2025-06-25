from psycopg_pool import ConnectionPool
from psycopg import Connection
from psycopg.rows import TupleRow

class PoolSettings:
    def __init__(self, user: str, password: str, host: str, port: int, dbname: str, \
                 appname: str, min_size: int, max_size: int, timeout: float):
        self.user:str = user
        self.password:str = password
        self.host:str = host
        self.port:int = port
        self.dbname:str = dbname
        self.appname:str = appname
        self.min_size:int = min_size
        self.max_size:int = max_size
        self.timeout:float = timeout

def test_connection(pool: ConnectionPool[Connection[TupleRow]]) -> str:
    # Check connection
    # conn: Connection[TupleRow] = None
    try:
        # Test connection by executing a simple query
        conn = pool.getconn()  # Get a connection from the pool
        if conn is None:
            raise ValueError("Failed to get a connection from the pool")
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            dbversion = cur.fetchone()
        # Commit changes (if any)
        conn.commit()
        # Return connection to pool
        pool.putconn(conn)
    except Exception as e:
        raise ValueError(e)
   
    return str(dbversion)


def connect(settings: PoolSettings) -> ConnectionPool[Connection[TupleRow]]:
    # Check pool settings
    if settings.min_size < 0 or settings.max_size < 0 or settings.timeout < 0:
        raise ValueError("Pool settings must be non-negative")
    if settings.min_size > settings.max_size:
        raise ValueError("Minimum pool size must be less than or equal to maximum pool size")
    if settings.min_size > 100 or settings.max_size > 100:
        raise ValueError("Pool settings must be less than or equal to 100")
    if settings.timeout > 60:
        raise ValueError("Timeout must be less than or equal to 60 seconds")
    if settings.min_size < 1:
        raise ValueError("Minimum pool size must be greater than or equal to 1")
    
    # Create connection string
     # Build connection string
    connstr =  f"user={settings.user} " \
        f"password={settings.password} " \
        f"host={settings.host} " \
        f"port={settings.port} " \
        f"dbname={settings.dbname} " \
        f"application_name='{settings.appname}'"
    
    # Create pool 
    pool: ConnectionPool[Connection[TupleRow]] = \
        ConnectionPool(conninfo=connstr, min_size=settings.min_size, max_size=settings.max_size, timeout=settings.timeout)
  
    # Return ConnectionPool
    return pool


# Close pool
def close(pool: ConnectionPool):
    try:
        pool.close()
    except Exception as e:
        raise ValueError(e)
    return
    
    
