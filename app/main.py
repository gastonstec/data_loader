from config import DBSettings
# import subprocess
# import sys
import database as db
import uuid
from services.pbx_adptx_calls import PBXAdaptixService



db_settings = db.PoolSettings(user=DBSettings.user, password=DBSettings.password, host=DBSettings.host, \
                             port=DBSettings.port, dbname=DBSettings.dbname, appname=DBSettings.appname, \
                                 min_size=DBSettings.min_size, max_size=DBSettings.max_size, timeout=DBSettings.timeout)


pool = db.connect(db_settings)
db_version = db.test_connection(pool)
# Print the database version
print(f"Connected to database version: {db_version}")

# Example usage of PBXAdaptixService to load calls
# Ensure the database connection is established before calling the service
pbx = PBXAdaptixService()

# x = pbx.pbx_calls_load(uri="d:\\datospbx\\CDRReport-202506-WHRc.csv", pool=pool, process_id='c69e54f2-84a2-4e63-bf57-e0e3991a2f39')
x = pbx.pbx_calls_load(uri="d:\\datospbx\\CDRReport-202411-202505.csv", pool=pool, process_id='afb7be06-1c65-4953-8d9b-a83c6bbaff7a')

try:
    db.close(pool)
except Exception as e:
    print(f"Error closing the database connection: {e}")
print("Process completed successfully.")
