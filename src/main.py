import logging
import config
# import subprocess
# import sys
from services.pbx_adptx_calls import pbx_load_data

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

x = pbx_load_data("d:\datospbx\CDRReport-202411-202505.csv")