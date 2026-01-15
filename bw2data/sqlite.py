import pickle

from peewee import BlobField, SqliteDatabase, Model
import playhouse.sqlite_ext as sqlite
import orjson
from bw2data.logs import stdout_feedback_logger


class PickleField(BlobField):
    def db_value(self, value):
        return super(PickleField, self).db_value(pickle.dumps(value, protocol=4))

    def python_value(self, value):
        return pickle.loads(bytes(value))


class SubstitutableDatabase:
    def __init__(self, filepath, tables):
        self._filepath = filepath
        self._tables = tables
        self._database = self._create_database()

    def _create_database(self):
        db = SqliteDatabase(self._filepath)
        for model in self._tables:
            model.bind(db, bind_refs=False, bind_backrefs=False)
        db.connect()
        db.create_tables(self._tables)
        return db

    @property
    def db(self):
        return self._database

    def change_path(self, filepath):
        self.db.close()
        self._filepath = filepath
        self._database = self._create_database()

    def atomic(self):
        return self.db.atomic()

    def execute_sql(self, *args, **kwargs):
        return self.db.execute_sql(*args, **kwargs)

    def transaction(self):
        return self.db.transaction()

    def vacuum(self):
        stdout_feedback_logger.info("Vacuuming database ")
        self.execute_sql("VACUUM;")


class FastJSONField(sqlite.JSONField):
    """Json field using orjson for faster serialization"""
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            json_loads=FastJSONField._loads,
            json_dumps=FastJSONField._dumps,
            **kwargs)

    @classmethod
    def _loads(cls, value):
        return orjson.loads(value)

    @classmethod
    def _dumps(cls, value):
        return orjson.dumps(value).decode("utf-8")


class CleanJSONField(FastJSONField):
    """JSON Field that deletes unwanted fields before saving to DB"""

    def setup_model(self, model_class):
        """Called after setup of the model because we can't circular reference a class in construction"""
        self.remove_fields = [f for f in model_class._meta.fields if f not in ['data', "id"]]

        # Add extra mapping from the Meta attribute of the model
        self.remove_fields.extend(list(model_class._meta.extra_data_mapping.keys()))


    def db_value(self, dic):
        if dic is None:
            return None
        cleaned = dic.copy()
        for field in self.remove_fields:
            cleaned.pop(field, None)
        return super().db_value(cleaned)


def spread_data_into_fields(model_class:type[Model], instance):
    """Called before save to DB, to put the fields of 'data' into proper fields."""

    if isinstance(instance, Model):
        # This should accomodate instance being either a Dataset instance of a dict (as called by insert_many)
        instance = instance.__data__

    data = instance["data"]

    if not data:
        return
    for key, value in data.items():
        if key in model_class._meta.fields:
            instance[key] = value

    # Process extra mapping
    for data_key, attr in model_class._meta.extra_data_mapping.items():
        val = data.get(data_key)
        if val is not None:
            if isinstance(attr, tuple):
                for key, val in zip(attr, val):
                    instance[key] = val
            else:
                instance[attr] = val


def add_field_into_data(model_class, instance):
    """Called after load from DB to put back  attributes of Peewee Dataset into data """
    if not instance.data:
        instance.data = {}
    for key in model_class._meta.fields:
        if key in  ["data", "id"]:
            continue
        val = getattr(instance, key)
        if val is not None:
            instance.data[key] = getattr(instance, key)

    # Process extra mapping
    for data_key, attr in model_class._meta.extra_data_mapping.items():
        if isinstance(attr, tuple):
            val = tuple(getattr(instance, key) for key in attr)
        else:
            val = getattr(instance, attr)
        instance.data[data_key] = val





