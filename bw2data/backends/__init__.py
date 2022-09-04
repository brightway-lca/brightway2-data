import os

from .. import config
from ..project import projects
from ..sqlite import SubstitutableDatabase
from .schema import ActivityDataset, ExchangeDataset, get_id, Location
from .base import SQLiteBackend

sqlite3_lci_db = SubstitutableDatabase(
    projects.dir / "lci" / "databases.db",
    [ActivityDataset, ExchangeDataset, Location, SQLiteBackend],
)

from .proxies import Activity, Exchange
from .utils import convert_backend
from .lcia import Method

config.sqlite3_databases.append(
    (
        os.path.join("lci", "databases.db"),
        sqlite3_lci_db,
    )
)

Node = Activity
Edge = Exchange
