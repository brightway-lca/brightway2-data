from bw2data.backends import Activity, SQLiteBackend
from bw2data.backends.iotable import IOTableBackend
from bw2data.backends.iotable.proxies import IOTableActivity
from bw2data.graph import GraphBackend
from bw2data.graph.proxies import GraphNode

DATABASE_BACKEND_MAPPING = {
    "sqlite": SQLiteBackend,
    "iotable": IOTableBackend,
    "graph": GraphBackend,
}

NODE_PROCESS_CLASS_MAPPING = {
    "sqlite": Activity,
    "iotable": IOTableActivity,
    "graph": GraphNode,
}
