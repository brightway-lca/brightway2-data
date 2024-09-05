from bw2data.backends import Activity, SQLiteBackend
from bw2data.backends.iotable import IOTableBackend
from bw2data.backends.iotable.proxies import IOTableActivity

DATABASE_BACKEND_MAPPING = {"sqlite": SQLiteBackend, "iotable": IOTableBackend}

NODE_PROCESS_CLASS_MAPPING = {
    "sqlite": Activity,
    "iotable": IOTableActivity,
}
