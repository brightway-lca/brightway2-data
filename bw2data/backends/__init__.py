import os

from bw2data import config
from bw2data.project import projects
from bw2data.sqlite import SubstitutableDatabase
from bw2data.backends.schema import ActivityDataset, ExchangeDataset, get_id

sqlite3_lci_db = SubstitutableDatabase(
    projects.dir / "lci" / "databases.db",
    [ActivityDataset, ExchangeDataset],
)

from bw2data.backends.base import SQLiteBackend
from bw2data.backends.proxies import Activity, Exchange
from bw2data.backends.utils import convert_backend

config.sqlite3_databases.append(
    (
        os.path.join("lci", "databases.db"),
        sqlite3_lci_db,
    )
)

Node = Activity
Edge = Exchange
