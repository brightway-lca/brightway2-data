from contextlib import closing

from peewee import (
    BlobField,
    BooleanField,
    CompositeKey,
    FloatField,
    DatabaseError,
    DataError,
    DoesNotExist,
    FieldAccessor,
    ForeignKeyField,
    IntegerField,
    IntegrityError,
    InterfaceError,
    InternalError,
    Model,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    TextField,
    fn,
)

from bw2data.errors import EXTENSION_ERROR_PREFIX, ReadOnlyExtensionError, UnknownObject
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
from bw2data.sqlite import (
    ZSTD_DICTIONARY_TABLE,
    CompressedJSONField,
    JSONField,
    PickleField,
    TupleNameField,
)

#: Real (on-disk) table names of the two LCI tables. Pinned explicitly so that they never
#: depend on peewee's naming rules; raw SQL and the extension overlay refer to these.
ACTIVITY_TABLE = "activitydataset"
EXCHANGE_TABLE = "exchangedataset"


class _SavedInTransaction:
    """Mixin for models with a ``CompressedJSONField``: ``save()`` runs in a transaction.

    The value is only compressed with the active zstd dictionary inside a transaction; see
    ``bw2data.sqlite.ZstdDictionaries``. The transaction takes the write lock when it begins
    (``IMMEDIATE``), before the active dictionary is read, so that the write itself can't
    fail afterwards because another process is writing. Inside a transaction of the caller
    nothing is added, and the lock is the caller's."""

    def save(self, **kwargs):
        database = self._meta.database
        if database.in_transaction() or database.transaction_token() is not None:
            return super().save(**kwargs)
        with database.atomic("IMMEDIATE"):
            return super().save(**kwargs)


class ActivityDataset(_SavedInTransaction, SnowflakeIDBaseClass):
    data = CompressedJSONField()  # Canonical, except for other C fields
    code = TextField()  # Canonical
    database = TextField()  # Canonical
    location = TextField(null=True)  # Reset from `data`
    name = TextField(null=True)  # Reset from `data`
    product = TextField(null=True)  # Reset from `data`
    type = TextField(null=True)  # Reset from `data`

    class Meta:
        table_name = ACTIVITY_TABLE

    @property
    def key(self):
        return (self.database, self.code)


#: Keys of exchange data which aren't stored in the ``data`` column, because the key columns
#: of ``ExchangeDataset`` already hold them. See ``ExchangeDataField``.
EXCHANGE_KEYS = ("input", "output")


def add_exchange_keys(data: dict, input_database, input_code, output_database, output_code) -> dict:
    """Put ``input`` and ``output`` into exchange ``data`` (in place), from the key columns.

    The columns are canonical, so they replace any ``input`` or ``output`` already in
    ``data``. A key is only set when both of its columns are known; with one of them ``None``
    (e.g. a column which wasn't selected) it is left as it is. Returns ``data``."""
    if input_database is not None and input_code is not None:
        data["input"] = (input_database, input_code)
    if output_database is not None and output_code is not None:
        data["output"] = (output_database, output_code)
    return data


class _ExchangeDataAccessor(FieldAccessor):
    """Gives the ``data`` of an ``ExchangeDataset`` read from the database its ``input`` and
    ``output``, from the key columns of the same instance.

    This happens once, the first time ``data`` is read, because the key columns may be set
    after ``data`` while a row is loaded. Later reads return the dict as it is, so changes
    made to it in memory stay. ``data`` which was assigned in memory is never changed: not
    on an instance built in Python, and not after assigning to ``data`` of a loaded one."""

    def __get__(self, instance, instance_type=None):
        if instance is None:
            return self.field
        row = instance.__data__
        value = row.get(self.name)
        if getattr(instance, "_keys_pending", False):
            instance._keys_pending = False
            # peewee clears `_dirty` after loading a row, so a dirty `data` was assigned later
            if isinstance(value, dict) and self.name not in instance._dirty:
                add_exchange_keys(
                    value,
                    row.get("input_database"),
                    row.get("input_code"),
                    row.get("output_database"),
                    row.get("output_code"),
                )
        return value


class ExchangeDataField(CompressedJSONField):
    """``CompressedJSONField`` for ``ExchangeDataset.data``, which doesn't store ``input`` and
    ``output``. They are already in the key columns, and as tuples they made every row a
    tagged value, which is decoded on the slow path (see ``bw2data.codec.loads``).

    Rows written by older versions still hold both keys. They are read as they are; the key
    columns win wherever the two differ, when a row is loaded as a model instance."""

    accessor_class = _ExchangeDataAccessor

    def encode(self, value) -> bytes:
        if isinstance(value, dict) and any(key in value for key in EXCHANGE_KEYS):
            value = {k: v for k, v in value.items() if k not in EXCHANGE_KEYS}
        return super().encode(value)


class ExchangeDataset(_SavedInTransaction, SnowflakeIDBaseClass):
    # Canonical, except for other C fields. Without `input` and `output`; see `ExchangeDataField`
    data = ExchangeDataField()
    input_code = TextField()  # Canonical
    input_database = TextField()  # Canonical
    output_code = TextField()  # Canonical
    output_database = TextField()  # Canonical
    type = TextField()  # Reset from `data`

    class Meta:
        table_name = EXCHANGE_TABLE

    def __init__(self, *args, **kwargs):
        # peewee passes `__no_default__` for every instance it builds from a row, and only
        # then. These get `input` and `output` in `data`; see `_ExchangeDataAccessor`.
        self._keys_pending = bool(kwargs.get("__no_default__"))
        super().__init__(*args, **kwargs)


#: The tables with a ``CompressedJSONField``, and the name of its column
COMPRESSED_COLUMNS = {ACTIVITY_TABLE: "data", EXCHANGE_TABLE: "data"}

#: Indexes on the natural keys of activities and exchanges. Not created with the tables: a bulk
#: ``Database.write`` drops and adds them (``SQLiteBackend._add_indices``)
LCI_INDEX_SQL = (
    f'CREATE UNIQUE INDEX IF NOT EXISTS main."activitydataset_key" ON "{ACTIVITY_TABLE}" '
    '("database", "code")',
    f'CREATE INDEX IF NOT EXISTS main."exchangedataset_input" ON "{EXCHANGE_TABLE}" '
    '("input_database", "input_code")',
    f'CREATE INDEX IF NOT EXISTS main."exchangedataset_output" ON "{EXCHANGE_TABLE}" '
    '("output_database", "output_code")',
)


class ZstdDictionary(Model):
    """A zstd dictionary used to compress the ``data`` of activities and exchanges.

    ``id`` is the dictionary id, which zstd writes in the header of every frame compressed
    with it. Read and written through ``bw2data.sqlite.ZstdDictionaries``. At most one row is
    ``active``: new values are compressed with it. Older dictionaries are kept while values
    use them; unused ones are deleted before ``VACUUM``."""

    id = IntegerField(primary_key=True)
    content = BlobField()
    created = TextField(null=True)  # ISO timestamp string
    active = BooleanField(default=False)

    class Meta:
        table_name = ZSTD_DICTIONARY_TABLE


ZstdDictionary.add_index(
    ZstdDictionary.index(
        ZstdDictionary.active,
        unique=True,
        where=(ZstdDictionary.active == True),  # noqa: E712
        name="zstddictionary_one_active",
    )
)


class DatabaseRecord(Model):
    """Registry record for a registered LCI database. Stored in the per-project `lci/databases.db`.

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

    class Meta:
        table_name = "databaserecord"


class _LCIARecord(Model):
    """Registry record of an LCIA method, weighting or normalization; one table per kind.

    ``id`` is a snowflake id: rows which refer to a record (e.g. its factors) use it, and it
    doesn't collide with the ids in another project's file. The natural key is ``name``, the
    tuple identifier, stored as a compact JSON list. Metadata keys other than the columns are
    stored in ``extra``; all columns except ``id`` and ``name`` are nullable, ``NULL`` meaning
    the key isn't set."""

    id = IntegerField(primary_key=True)
    name = TupleNameField(unique=True)
    abbreviation = TextField(null=True)
    extra = JSONField(null=True)
    #: Whether the object's data (characterization factors, normalization factors or
    #: weighting) is stored in SQLite. Not part of the metadata dict.
    has_data = BooleanField(default=False)


class MethodRecord(_LCIARecord):
    """Registry record of an LCIA method. Read and written through ``bw2data.methods``."""

    num_cfs = IntegerField(null=True)
    geocollections = JSONField(null=True)

    class Meta:
        table_name = "method"


class WeightingRecord(_LCIARecord):
    """Registry record of an LCIA weighting. Read and written through ``bw2data.weightings``."""

    class Meta:
        table_name = "weighting"


class NormalizationRecord(_LCIARecord):
    """Registry record of an LCIA normalization. Read and written through
    ``bw2data.normalizations``."""

    class Meta:
        table_name = "normalization"


class _Factor(Model):
    """One value of an LCIA object: a characterization factor, a normalization factor or a
    weighting. One table per kind; rows refer to their object by its snowflake id, and are
    deleted with it.

    ``position`` keeps the order in which the rows were written. The value is ``amount``
    and, if it was given as an uncertainty dictionary, the other keys of that dictionary in
    ``uncertainty`` (``{}`` for a dictionary with only ``amount``; ``NULL`` for a plain
    number)."""

    position = IntegerField()
    amount = FloatField(null=True)
    uncertainty = JSONField(null=True)


class CharacterizationFactor(_Factor):
    """Characterization factor of an LCIA method. Read and written through
    ``Method(name).load()`` and ``Method(name).write()``.

    ``flow`` is an activity id, not a foreign key: the activity can be in another (attached)
    file, or deleted. The location is stored by name, never as a geomapping id: ``location``
    alone for a bare location string, ``geocollection`` and ``location`` for a
    ``(geocollection, location)`` tuple, both ``NULL`` for the global location."""

    method = ForeignKeyField(
        MethodRecord, on_delete="CASCADE", column_name="method_id", index=False
    )
    flow = IntegerField(index=True)
    geocollection = TextField(null=True)
    location = TextField(null=True)

    class Meta:
        table_name = "characterizationfactor"
        primary_key = CompositeKey("method", "position")


class NormalizationFactor(_Factor):
    """Normalization factor of an LCIA normalization. ``flow`` is an activity id, as in
    ``CharacterizationFactor``."""

    normalization = ForeignKeyField(
        NormalizationRecord, on_delete="CASCADE", column_name="normalization_id", index=False
    )
    flow = IntegerField(index=True)

    class Meta:
        table_name = "normalizationfactor"
        primary_key = CompositeKey("normalization", "position")


class WeightingFactor(_Factor):
    """The weighting of an LCIA weighting (a single row)."""

    weighting = ForeignKeyField(
        WeightingRecord, on_delete="CASCADE", column_name="weighting_id", index=False
    )

    class Meta:
        table_name = "weightingfactor"
        primary_key = CompositeKey("weighting", "position")


class ExtensionDatabaseState(Model):
    """Project-local state of a database stored in a read-only extension.

    A database record in an extension can't be changed, but some of its fields change when the
    project uses the database: ``dirty``, ``searchable`` and ``modified``. Changes to these are
    stored here, in the project's own file, and shown in place of the extension's values (see
    ``bw2data.backends.extensions``). ``NULL`` means not set locally.

    ``processed`` is the time when the project last processed the database, as an ISO
    timestamp. For a database in the project's own file this is the ``processed`` key of
    ``DatabaseRecord.extra``; the ``extra`` of an extension's record is read-only.
    """

    name = TextField(primary_key=True)
    dirty = BooleanField(null=True)
    searchable = BooleanField(null=True)
    modified = TextField(null=True)
    processed = TextField(null=True)

    class Meta:
        table_name = "extensiondatabasestate"


class Preference(Model):
    """One project preference. Read and written through ``bw2data.preferences``.

    Stored in the per-project ``lci/databases.db``. Previously stored in
    ``preferences.pickle``; that file is migrated by schema migration 1 (see
    ``bw2data.backends.migrations``). Values can be any picklable object."""

    key = TextField(primary_key=True)
    value = PickleField()

    class Meta:
        table_name = "preference"


class Geocollection(Model):
    """A named collection of spatial units, e.g. ``"world"`` or ``"ecoinvent"``.

    Stored in the per-project ``lci/databases.db``. Rows are created automatically when
    locations in a new collection are added to ``geomapping``. ``extra`` holds arbitrary
    metadata about the collection (e.g. the spatial data file used by ``bw2regional``).

    Ids are snowflake ids, as for ``GeoMappingRecord``; ids assigned before (small integers
    from an SQLite sequence) are kept.
    """

    id = IntegerField(primary_key=True)
    name = TextField(unique=True)
    extra = JSONField(null=True)

    class Meta:
        table_name = "geocollection"


class GeoMappingRecord(Model):
    """Integer id for a location, used in processed datapackages. Read through ``geomapping``.

    A location is either a bare string (``geocollection`` is ``NULL``), or a feature id in a
    ``Geocollection``. New ids are snowflake ids (see ``bw2data.snowflake_ids``), like the ids
    of activities and exchanges: they are never reused after a location is deleted, and don't
    collide with the ids in another project's file. Ids assigned before snowflake ids were
    used (small integers from an SQLite sequence) are kept, as processed datapackages refer
    to them.
    """

    id = IntegerField(primary_key=True)
    geocollection = ForeignKeyField(
        Geocollection, null=True, on_delete="RESTRICT", backref="locations"
    )
    location = TextField()

    class Meta:
        table_name = "geomappingrecord"


# NULLs are distinct in SQLite unique indices, so a plain unique index on
# ``(geocollection, location)`` would allow duplicate bare-string locations.
GeoMappingRecord.add_index(
    GeoMappingRecord.index(
        fn.COALESCE(GeoMappingRecord.geocollection, 0),
        GeoMappingRecord.location,
        unique=True,
        name="geomappingrecord_collection_location",
    )
)


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


# The DB-API error classes, which peewee and every driver name alike
_PEEWEE_ERRORS = {
    cls.__name__: cls
    for cls in (
        DatabaseError,
        DataError,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
        ProgrammingError,
    )
}


def _as_peewee_error(exc: Exception) -> Exception:
    """Return the peewee error for a driver error, as ``execute_sql`` would raise it.

    Peewee has no public way to run ``executemany`` with its error translation, so this
    matches the driver's error class, or its nearest DB-API base class, by name. An error
    with no peewee counterpart is returned as it is.
    """
    for cls in type(exc).__mro__:
        if cls.__name__ in _PEEWEE_ERRORS:
            error = _PEEWEE_ERRORS[cls.__name__](exc, *exc.args)
            if isinstance(error, IntegrityError) and EXTENSION_ERROR_PREFIX in str(exc):
                # Refused by a trigger of the read-only extensions, as in
                # ``BWSqliteDatabase.execute_sql``
                return ReadOnlyExtensionError(str(exc))
            return error
    return exc


def _insert_many(model, rows: list) -> None:
    """Insert ``rows`` with one raw ``executemany``, much faster than ``model.insert_many``.

    ``rows`` are dicts from ``dict_as_activitydataset`` or ``dict_as_exchangedataset``, all
    with the same keys. Each value goes through its field's ``db_value``, as in
    ``insert_many``. Must be called within a transaction.
    """
    if not rows:
        return
    fields = [model._meta.fields[name] for name in rows[0]]
    sql = 'INSERT INTO "{}" ({}) VALUES ({})'.format(
        model._meta.table_name,
        ", ".join(f'"{field.column_name}"' for field in fields),
        ", ".join("?" * len(fields)),
    )
    converters = [(field.name, field.db_value) for field in fields]
    connection = model._meta.database.connection()
    try:
        # ``closing`` frees the cursor straight away instead of at garbage collection
        with closing(connection.cursor()) as cursor:
            cursor.executemany(
                sql, ([convert(row[name]) for name, convert in converters] for row in rows)
            )
    except connection.Error as exc:
        # Driver errors come out as peewee's, as they do from ``execute_sql``
        error = _as_peewee_error(exc)
        if error is exc:
            raise
        raise error from exc
