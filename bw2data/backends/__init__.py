import os

from .. import config
from ..project import projects
from ..sqlite import SubstitutableDatabase
from .base import (  # SQLiteBackend just for backwards compatibility
    Database,
    SQLiteBackend,
)
from .schema import ActivityDataset, ExchangeDataset, get_id

sqlite3_lci_db = SubstitutableDatabase(
    projects.dir / "lci" / "databases.db",
    [ActivityDataset, ExchangeDataset, Database],
)

from .proxies import Activity, Exchange

config.sqlite3_databases.append(
    (
        os.path.join("lci", "databases.db"),
        sqlite3_lci_db,
    )
)

Node = Activity
Edge = Exchange
