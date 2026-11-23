import os

from bw2data import config
from bw2data.project import projects
from bw2data.sqlite import SubstitutableDatabase, zstd_dictionaries
from bw2data.backends.migrations import migrate
from bw2data.backends.schema import (
    COMPRESSED_COLUMNS,
    ActivityDataset,
    CharacterizationFactor,
    DatabaseRecord,
    ExchangeDataset,
    GeoMappingRecord,
    Geocollection,
    MethodRecord,
    NormalizationFactor,
    NormalizationRecord,
    Preference,
    WeightingFactor,
    WeightingRecord,
    ZstdDictionary,
    get_id,
)

#: Page size of ``lci/databases.db``, in bytes. Activity and exchange rows are around 1 KB,
#: which leaves a lot of unused space at the end of SQLite's default 4 KB pages; with 16 KB
#: pages, ecoinvent 3.13 takes about 11% less space. Peewee sets the pragma on every
#: connection: a new file gets this page size before its first table is created, and an
#: existing file is converted the next time it is vacuumed. The cost is a larger minimum
#: size, as every table and index takes at least one page: an empty project's file grows
#: from 48 KB to 192 KB. A file which another tool switched to WAL journal mode keeps its
#: page size, because SQLite can't change it in that mode.
LCI_PAGE_SIZE = 16384

sqlite3_lci_db = SubstitutableDatabase(
    projects.dir / "lci" / "databases.db",
    [
        ActivityDataset,
        ExchangeDataset,
        ZstdDictionary,
        DatabaseRecord,
        Geocollection,
        GeoMappingRecord,
        Preference,
        MethodRecord,
        WeightingRecord,
        NormalizationRecord,
        CharacterizationFactor,
        WeightingFactor,
        NormalizationFactor,
    ],
    # ``page_size`` has no effect in WAL journal mode, so keep the default rollback journal
    pragmas={"foreign_keys": 1, "page_size": LCI_PAGE_SIZE},
    # ``projects.dir`` already points at the new project when the file is switched
    on_open=lambda db: migrate(db, projects.dir),
    # A project which is read-only, or in use by another process, can still be opened after
    # an upgrade of bw2data which adds tables
    postpone_tables=True,
    # Dictionaries of values which were deleted since
    before_vacuum=lambda db: zstd_dictionaries(db).remove_unused(COMPRESSED_COLUMNS),
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
