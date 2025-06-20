from config import DBSettings
# import subprocess
# import sys
import database as db
import uuid
from services.pbx_adptx_calls import PBXAdaptixService

# command = ["foo", "bar"]
# if sys.platform == "win32":
#     subprocess.Popen(
#         command,
#         creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS,
#         stdin=subprocess.DEVNULL,
#         stdout=subprocess.DEVNULL,
#         stderr=subprocess.DEVNULL,
#         close_fds=True,
#     )
# else:
#     subprocess.Popen(
#         command,
#         start_new_session=True,
#         stdin=subprocess.DEVNULL,
#         stdout=subprocess.DEVNULL,
#         stderr=subprocess.DEVNULL,
#         close_fds=True,
#     )

db_settings = db.PoolSettings(user=DBSettings.user, password=DBSettings.password, host=DBSettings.host, \
                             port=DBSettings.port, dbname=DBSettings.dbname, appname=DBSettings.appname, \
                                 min_size=DBSettings.min_size, max_size=DBSettings.max_size, timeout=DBSettings.timeout)


pool, dbVersion = db.connect(db_settings)

# Example usage of PBXAdaptixService to load calls
# Ensure the database connection is established before calling the service
pbx = PBXAdaptixService()

x = pbx.pbx_calls_load(uri="d:\\datospbx\\CDRReport-202411-202505.csv", pool=pool, process_id=str(uuid.uuid4()))

db.close(pool)
