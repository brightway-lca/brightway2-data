from .backends import SQLiteBackend
from .backends.iotable import IOTableBackend
from .backends import Activity
from .backends.iotable.proxies import IOTableActivity


DATABASE_BACKEND_MAPPING = {
    "sqlite": SQLiteBackend,
    "iotable": IOTableBackend
}

NODE_PROCESS_CLASS_MAPPING = {
    'sqlite': Activity,
    'iotable': IOTableActivity,
}
