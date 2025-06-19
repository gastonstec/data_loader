from datetime import datetime, timezone
from psycopg_pool import ConnectionPool
import pandas as pd

TIMEZONE:str = "America/Mexico_City"

class PBXAdaptixCall:
    def __init__(self, pool: ConnectionPool):
        self.pool = pool

    @staticmethod
    def get_duracion_seg(duracion: str) -> str:
        if duracion == "":
            return "0"
        try:
            dseg = duracion[0:duracion.find("s")]
        except ValueError as e:
            raise ValueError(f"Error extracting duration in seconds from '{duracion}': {e}")
        return dseg
    
    @staticmethod
    def get_extension(destino: str) -> str:
        if destino == "":
            return ""
        try:
            ext = destino[destino.find("/")+1:destino.find("-")]
        except ValueError as e:
            raise ValueError(f"Error extracting extension from destino '{destino}': {e}")
        return ext

    def insert_bulk_to_db(self, conn, process_id: str, df: pd.DataFrame) -> int:
        """
        Insert data into the database using a connection.
        """
        if conn is None:
            raise ValueError("Connection is None. Cannot insert data.")
        
        with conn.transaction():
            with conn.cursor() as cursor:
                cursor.execute(f"SET TIMEZONE TO '{TIMEZONE}'")
                for row in df.itertuples():
                    
                    # Get the extension and duration in seconds from the row data
                    extension = self.get_extension(str(row[6]))
                    duracion_seg = self.get_duracion_seg(str(row[8]))

                    # Prepare the SQL statement with the row data
                    sql_statement = (
                        f"INSERT INTO gtim_pbx_adaptix_calls (process_id,fecha,caller_id,grupo_timbrado,destino,canal_origen,codigo_cuenta,canal_destino,estado,duracion,duracion_seg,extension) "
                        f"VALUES ('{process_id}','{row[0]}','{row[1]}','{row[2]}','{row[3]}','{row[4]}','{row[5]}','{row[6]}','{row[7]}','{row[8]}',{duracion_seg},'{extension}')"
                    )
                    
                    # Execute the SQL statement
                    try:
                        cursor.execute(sql_statement)
                    except Exception as e:
                        raise ValueError(f"Error executing SQL statement: {e}")

        print(f"Data inserted successfully for process_id: {process_id}")

    # Insert PBX calls data into the database.
    def insert_bulk(self, process_id: str, df: pd.DataFrame):
        # Validate the DataFrame
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame.")
        if df.empty:
            raise ValueError("DataFrame is empty. No data to insert.")
                    
        # Get a connection from the pool
        try:
            conn = self.pool.getconn()
        except Exception as e:
            raise ValueError(f"Error getting connection from pool: {e}")
        
        # Check if the connection is valid
        if conn is None:
            raise ValueError("Failed to obtain a connection from the pool.")
        
        # Insert data into the database
        try:
            self.insert_bulk_to_db(conn=conn, df=df, process_id=process_id)
        except Exception as e:
            raise ValueError(f"Error inserting data into the database: {e}")
        finally:
            self.pool.putconn(conn)