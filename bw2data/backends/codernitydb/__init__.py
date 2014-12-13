from ... import config
from CodernityDB.database import Database as CDatabase
import hashlib

def uniencode(s):
    return hashlib.md5(unicode(s).encode('utf-8')).digest()

from .ds_indices import (
    CodeIndex,
    DatabaseNameIndex,
    KeyIndex,
    LocationIndex,
    ReferenceProductIndex,
    UnitIndex,
)
from .exc_indices import (
    ExchangeTypeIndex,
    InputDatabaseIndex,
    InputKeyIndex,
    OutputDatabaseIndex,
    OutputKeyIndex,
)

cdb_datasets = CDatabase(config.request_dir("cdb-datasets"))
cdb_exchanges = CDatabase(config.request_dir("cdb-exchanges"))

try:
    cdb_datasets.open()
    cdb_exchanges.open()
except:
    cdb_datasets.create()
    cdb_exchanges.create()
    cdb_datasets.add_index(KeyIndex(cdb_datasets.path, "key"))
    cdb_datasets.add_index(DatabaseNameIndex(cdb_datasets.path, "database"))
    cdb_datasets.add_index(CodeIndex(cdb_datasets.path, "code"))
    cdb_datasets.add_index(LocationIndex(cdb_datasets.path, "location"))
    cdb_datasets.add_index(ReferenceProductIndex(cdb_datasets.path, "product"))
    cdb_datasets.add_index(UnitIndex(cdb_datasets.path, "unit"))
    cdb_exchanges.add_index(InputKeyIndex(cdb_exchanges.path, "input"))
    cdb_exchanges.add_index(OutputKeyIndex(cdb_exchanges.path, "output"))
    cdb_exchanges.add_index(InputDatabaseIndex(cdb_exchanges.path, "input.d"))
    cdb_exchanges.add_index(OutputDatabaseIndex(cdb_exchanges.path, "output.d"))
    cdb_exchanges.add_index(ExchangeTypeIndex(cdb_exchanges.path, "type"))

from .database import CodernityDBBackend
