## Define package-level variables
PACKAGE_VERSION = "1.0.0"

## Control imports
from .db import PoolSettings, connect, close, test_connection