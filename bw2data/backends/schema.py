from peewee import DoesNotExist, Model, TextField

from ..errors import UnknownObject
from ..sqlite import PickleField, TupleJSONField


class ActivityDataset(Model):
    data = PickleField()
    code = TextField()
    database = TextField()
    location = TupleJSONField(null=True)
    name = TextField(null=True)
    product = TextField(null=True)
    type = TextField(null=True)

    @property
    def key(self):
        return (self.database, self.code)


class ExchangeDataset(Model):
    data = PickleField()
    input_code = TextField()
    input_database = TextField()
    output_code = TextField()
    output_database = TextField()
    type = TextField()


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
