from peewee import DoesNotExist, Model, TextField

from bw2data.errors import UnknownObject
from bw2data.sqlite import PickleField

import bw2data.signals as bwsignals

try:
    from typing import override
except ImportError:
    from typing_extensions import override


class SignaledDataset(Model):
    @override
    def save(self, *args, **kwargs):
        """Receives a mapper to convert the data to the expected dictionary format"""
        old = None
        try:
            old = type(self).get(type(self).id == self.id)
        except DoesNotExist:
            pass
        super().save(*args, **kwargs)
        bwsignals.database_saved.send(
            old=old,
            new=self,
        )


class ActivityDataset(SignaledDataset):
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


class ExchangeDataset(SignaledDataset):
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
