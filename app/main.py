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

x = pbx.pbx_calls_load(uri="d:\\datospbx\\CDRReport-202411-202505.csv", pool=pool, process_id='afb7be06-1c65-4953-8d9b-a83c6bbaff7a')

db.close(pool)
