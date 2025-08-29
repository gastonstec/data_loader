from psycopg_pool import ConnectionPool
from psycopg import Connection
from psycopg.rows import TupleRow, dict_row


# Class for managing PostgreSQL connections
# using psycopg_pool
class DBConnectionPool():
    # Initialize the database connection parameters
    # user: str, password: str, host: str, port: int, dbname: str, \
    # appname: str, min_size: int, max_size: int, timeout: float
    # min_size: Minimum number of connections in the pool
    # max_size: Maximum number of connections in the pool
    # timeout: Timeout for acquiring a connection from the pool in seconds
    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: int,
        dbname: str,
        appname: str,
        min_size: int,
        max_size: int,
        timeout: float
    ):
        self.user: str = user
        self.password: str = password
        self.host: str = host
        self.port: int = port
        self.dbname: str = dbname
        self.appname: str = appname
        self.min_size: int = min_size
        self.max_size: int = max_size
        self.timeout: float = timeout
        self.pool: ConnectionPool[Connection[TupleRow]]

    # Create connection pool
    def connect(self, timeout: float = 5):
        # Check pool selfs
        if self.min_size < 0 or self.max_size < 0 or self.timeout < 0:
            raise ValueError("Pool self must be non-negative")
        if self.min_size > self.max_size:
            raise ValueError(
                (
                    "Minimum pool size must be less than or equal to "
                    "maximum pool size"
                )
            )
        if self.min_size < 1 or self.max_size > 100:
            raise ValueError(
                "Pool size must be less than or equal to 100"
            )
        if self.timeout < 5 or self.timeout > 60:
            raise ValueError(
                "Timeout must be between 5 and 60 seconds"
            )

        # Build connection string
        connstr = f"user={self.user} " \
            f"password={self.password} " \
            f"host={self.host} " \
            f"port={self.port} " \
            f"dbname={self.dbname} " \
            f"application_name='{self.appname}'"

        try:
            # Create connection pool
            self.pool: ConnectionPool[Connection[TupleRow]] = \
                ConnectionPool(
                    conninfo=connstr,
                    min_size=self.min_size,
                    max_size=self.max_size,
                    timeout=self.timeout,
                    kwargs={"row_factory": dict_row}
                )
            # Open the connection pool
            self.pool.open(wait=True, timeout=timeout)
        except Exception as e:
            raise ValueError(f"Failed to create connection: {e}")

    # Test the connection by executing a simple query
    def test(self) -> str:
        try:
            # Get a connection from the pool
            conn = self.pool.getconn()
            # Check if the connection is valid
            if conn is None:
                raise ValueError("Failed to get a connection from the pool")
            # Execute a simple query to test the connection
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                dbversion = cur.fetchone()
            # Commit changes (if any)
            conn.commit()
            # Return connection to pool
            self.pool.putconn(conn)
        except Exception as e:
            raise ValueError(e)
        # Return the database version
        return str(dbversion)

    # Get a connection from the pool
    def get_conn(self) -> Connection[TupleRow]:
        try:
            # Get a connection from the pool
            conn = self.pool.getconn()
            if conn is None:
                raise ValueError("Failed to get a connection from the pool")
            return conn
        except Exception as e:
            raise ValueError(e)

    # Return a connection to the pool
    def put_conn(self, conn: Connection[TupleRow]):
        try:
            self.pool.putconn(conn)
        except Exception as e:
            raise ValueError(e)

    # Close connection pool
    def close(self, timeout: float = 15.0):
        try:
            # Close the connection pool
            self.pool.close(timeout=timeout)
        except Exception as e:
            raise ValueError(e)
