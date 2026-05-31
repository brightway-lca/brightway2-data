from peewee import BooleanField, DoesNotExist, IntegerField, Model, TextField

from bw2data.errors import UnknownObject
from bw2data.signals import (
    on_activity_code_change,
    on_activity_database_change,
    on_database_delete,
    on_database_reset,
    on_database_write,
    project_changed,
    signaleddataset_on_delete,
)
from bw2data.snowflake_ids import SnowflakeIDBaseClass
from bw2data.sqlite import JSONField, PickleField


class ActivityDataset(SnowflakeIDBaseClass):
    data = PickleField()  # Canonical, except for other C fields
    code = TextField()  # Canonical
    database = TextField()  # Canonical
    location = TextField(null=True)  # Reset from `data`
    name = TextField(null=True)  # Reset from `data`
    product = TextField(null=True)  # Reset from `data`
    type = TextField(null=True)  # Reset from `data`

    @property
    def key(self):
        return (self.database, self.code)


class ExchangeDataset(SnowflakeIDBaseClass):
    data = PickleField()  # Canonical, except for other C fields
    input_code = TextField()  # Canonical
    input_database = TextField()  # Canonical
    output_code = TextField()  # Canonical
    output_database = TextField()  # Canonical
    type = TextField()  # Reset from `data`


class DatabaseMetadata(Model):
    """Metadata for a registered LCI database. Stored in the per-project `lci/databases.db`.

    All columns are nullable. A ``NULL`` value means the field was never explicitly set,
    which matches the historical behaviour of ``databases.json`` where absent keys were
    simply not present in the dict.
    """

    name = TextField(primary_key=True)
    backend = TextField(null=True)
    depends = JSONField(null=True)
    dirty = BooleanField(null=True)
    version = IntegerField(null=True)
    modified = TextField(null=True)       # ISO timestamp string
    number = IntegerField(null=True)
    searchable = BooleanField(null=True)
    geocollections = JSONField(null=True)
    extra = JSONField(null=True)          # arbitrary user-defined fields


_get_id_cache: dict = {}


def get_id(key):
    if isinstance(key, int):
        try:
            ActivityDataset.get(ActivityDataset.id == key)
        except DoesNotExist:
            raise UnknownObject
        return key
    else:
        cache_key = (key[0], key[1])
        if cache_key in _get_id_cache:
            return _get_id_cache[cache_key]
        try:
            result = ActivityDataset.get(
                ActivityDataset.database == key[0], ActivityDataset.code == key[1]
            ).id
            _get_id_cache[cache_key] = result
            return result
        except DoesNotExist:
            raise UnknownObject


def _clear_get_id_cache(sender, **kwargs):
    _get_id_cache.clear()


def _remove_database_from_get_id_cache(sender, name: str, **kwargs):
    for k in [k for k in _get_id_cache if k[0] == name]:
        del _get_id_cache[k]


def _remove_activity_from_get_id_cache(sender, old=None, **kwargs):
    if isinstance(old, ActivityDataset):
        _get_id_cache.pop((old.database, old.code), None)


def _remove_changed_activity_key_from_get_id_cache(sender, old=None, **kwargs):
    if old is not None:
        for k in [k for k, v in _get_id_cache.items() if v == old["id"]]:
            del _get_id_cache[k]


project_changed.connect(_clear_get_id_cache)
on_database_delete.connect(_remove_database_from_get_id_cache)
on_database_reset.connect(_remove_database_from_get_id_cache)
on_database_write.connect(_remove_database_from_get_id_cache)
signaleddataset_on_delete.connect(_remove_activity_from_get_id_cache)
on_activity_database_change.connect(_remove_changed_activity_key_from_get_id_cache)
on_activity_code_change.connect(_remove_changed_activity_key_from_get_id_cache)
