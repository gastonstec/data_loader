# def pbx_calls_load(self, pool: ConnectionPool, process_id: str, uri: str):
#         # Load PBX data from CSV file
#         try:
#             df = self.pbx_calls_load_csv(uri)
#         except Exception as e:
#             logger.error(
#                 "Error loading PBX data from CSV file: {}".format(e)
#             )
#             raise ValueError(e)
#         # Calculate the number of rows in the DataFrame
#         num_rows = len(df)
#         logger.info("Number of rows to be loaded: {}".format(num_rows))
#         try:
#             pbxModel = PBXAdaptixCall(pool=pool)
#             pbxModel.insert_bulk(df=df, process_id=process_id)
#         except Exception as e:
#             logger.error("Error getting connection from pool: {}".format(e))
#             raise ValueError(e)
#         logger.info("Connection obtained from pool")
#         # Return processed DataFrame
#         del df
#         print("Number of rows loaded: {}".format(num_rows))
#         return num_rows

def start(base_folder, db_pool, duckdb_conn):
    print("Starting PBX Adaptix Load Pipeline")
