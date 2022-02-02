import itertools

from peewee import DoesNotExist, Model, TextField, chunked

from ..errors import UnknownObject
from ..sqlite import PickleField
from .. import MAX_SQLITE_PARAMETERS


class ActivityDataset(Model):
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

    class Meta:
        indexes = (
            (('database', 'code'), True),
        )
        legacy_table_names=True


class ExchangeDataset(Model):
    data = PickleField()  # Canonical, except for other C fields
    input_code = TextField()  # Canonical
    input_database = TextField()  # Canonical
    output_code = TextField()  # Canonical
    output_database = TextField()  # Canonical
    type = TextField()  # Reset from `data`

    class Meta:
        indexes = (
            (("input_database", "input_code"), False),
            (("output_database", "output_code"), False),
        )
        legacy_table_names=True


def get_id(key):
    if isinstance(key, int):
        return key
    else:
        try:
            return ActivityDataset.get(
                ActivityDataset.database == key[0], ActivityDataset.code == key[1]
            ).id
        except DoesNotExist:
            raise UnknownObject


class Location(Model):
    geocollection = TextField(null=True)
    name = TextField(null=False)

    def __str__(self):
        return "{}: {}|{}".format(self.id, self.geocollection, self.name)

    def __lt__(self, other):
        if not isinstance(other, Location):
            raise TypeError
        else:
            return str(self) < str(other)

    class Meta:
        indexes = (
            (('geocollection', 'name'), True),
        )

    @classmethod
    def initial_data(cls):
        cls.get_or_create(geocollection=None, name="GLO")

    @classmethod
    def from_key(cls, key):
        if isinstance(key, str):
            key = (None, key)
        return cls.get(cls.geocollection == key[0], cls.name == key[1])

    @classmethod
    def add_many(cls, keys):
        """Add an iterable of keys"""
        from . import sqlite3_lci_db

        for group, itr in itertools.groupby(cls._reformat_keys(keys), key=lambda x: x[0]):
            lst = [x[1] for x in itr]
            missing = set(lst).difference({o[0] for o in cls.select(cls.name).where((cls.geocollection == group) & (cls.name << lst)).tuples()})
            with sqlite3_lci_db.atomic():
                for batch in chunked(missing, int(MAX_SQLITE_PARAMETERS / 2.01)):
                    cls.insert_many(zip(itertools.repeat(group), missing), fields=(cls.geocollection, cls.name)).execute()

    @classmethod
    def delete_many(cls, keys):
        from . import sqlite3_lci_db

        for group, itr in itertools.groupby(cls._reformat_keys(keys), key=lambda x: x[0]):
            with sqlite3_lci_db.atomic():
                for batch in chunked([x[1] for x in itr], int(MAX_SQLITE_PARAMETERS) - 1):
                    cls.delete().where((cls.geocollection == group) & (cls.name << batch)).execute()

    @classmethod
    def _reformat_keys(cls, keys):
        reformatted = [(None, key) for key in keys if isinstance(key, str)] + [key for key in keys if isinstance(key, tuple)]

        other_keys = [key for key in keys if not isinstance(key, (tuple, str))]
        if other_keys:
            raise ValueError(f"Geomapping keys can only be strings or tuples; got: {other_keys}")

        return reformatted
