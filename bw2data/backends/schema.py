from peewee import DoesNotExist, Model, TextField, IntegerField

from bw2data.errors import UnknownObject
from bw2data.signals import SignaledDataset
from bw2data.sqlite import PickleField
from bw2data.snowflake_ids import snowflake_id_generator


class SnowflakeIDBaseClass(SignaledDataset):
    id = IntegerField(primary_key=True)

    def save(self, **kwargs):
        if self.id is None:
            # If the primary key is already present, peewee will make an `UPDATE` query.
            # This will have no effect if there isn't a matching row
            # https://docs.peewee-orm.com/en/latest/peewee/models.html#id4
            self.id = next(snowflake_id_generator)
            kwargs['force_insert'] = True
        super().save(**kwargs)


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


def get_id(key):
    if isinstance(key, int):
        try:
            ActivityDataset.get(ActivityDataset.id == key)
        except DoesNotExist:
            raise UnknownObject
        return key
    else:
        try:
            return ActivityDataset.get(
                ActivityDataset.database == key[0], ActivityDataset.code == key[1]
            ).id
        except DoesNotExist:
            raise UnknownObject
