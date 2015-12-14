# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import sqlite3_lci_db
from ... import databases, mapping, geomapping, config
from ...errors import ValidityError, NotAllowed
from ...project import writable_project
from ...proxies import ActivityProxyBase, ExchangeProxyBase
from ...search import IndexManager
from ...utils import get_activity
from .schema import ActivityDataset, ExchangeDataset
from .utils import dict_as_activitydataset, dict_as_exchangedataset
import copy
import collections
import warnings
import uuid


class Exchanges(collections.Iterable):
    """Iterator for exchanges with some additional methods"""
    def __init__(self, key, kind=None, reverse=False):
        self._key = key
        self._kind = kind
        if reverse:
            self._args = [
            ExchangeDataset.input_database == self._key[0],
            ExchangeDataset.input_code == self._key[1],
            # No production exchanges
            ExchangeDataset.output_database != self._key[0],
            ExchangeDataset.output_code != self._key[1],
        ]
        else:
            self._args = [
                ExchangeDataset.output_database == self._key[0],
                ExchangeDataset.output_code == self._key[1],
            ]
        if self._kind:
            self._args.append(ExchangeDataset.type == self._kind)

    def filter(self, expr):
        self._args.append(expr)

    @writable_project
    def delete(self):
        databases.set_dirty(self._key[0])
        ExchangeDataset.delete().where(*self._args).execute()

    def _get_queryset(self):
        return ExchangeDataset.select().where(*self._args)

    def __iter__(self):
        for obj in self._get_queryset():
            yield Exchange(obj)

    def __len__(self):
        return self._get_queryset().count()


class Activity(ActivityProxyBase):
    def __init__(self, document=None):
        if document is None:
            self._document = ActivityDataset()
            self._data = {}
        else:
            self._document = document
            self._data = self._document.data
            self._data['code'] = self._document.code
            self._data['database'] = self._document.database

    def __setitem__(self, key, value):
        if key == 'code':
            raise NotAllowed(
                "Use `change_code` method to change the activity code"
            )
        elif key == 'database':
            raise NotAllowed(
                "Use `change_database` method to change the activity database"
            )
        else:
            super(Activity, self).__setitem__(key, value)

    @property
    def key(self):
        return (self.get("database"), self.get("code"))

    @writable_project
    def delete(self):
        IndexManager().delete_dataset(self._data)
        self.exchanges().delete()
        self._document.delete_instance()
        self = None

    @writable_project
    def save(self):
        if not self.valid():
            raise ValidityError("This activity can't be saved for the "
                "following reasons\n\t* " + \
                "\n\t* ".join(self.valid(why=True)[1])
            )

        databases.set_dirty(self['database'])

        for key, value in dict_as_activitydataset(self._data).items():
            setattr(self._document, key, value)
        self._document.save()

        if databases[self['database']].get('searchable', True):
            IndexManager().update_dataset(self._data)

        if self.key not in mapping:
            mapping.add([self.key])
        if self.get('location') and self['location'] not in geomapping:
            geomapping.add([self['location']])

    def change_code(self, new_code):
        if self['code'] == new_code:
            return

        with sqlite3_lci_db.atomic() as txn:
            ActivityDataset.update(code=new_code).where(
                ActivityDataset.database == self['database'],
                ActivityDataset.code == self['code']
            ).execute()
            ExchangeDataset.update(output_code=new_code).where(
                ExchangeDataset.output_database == self['database'],
                ExchangeDataset.output_code == self['code'],
            ).execute()
            ExchangeDataset.update(input_code=new_code).where(
                ExchangeDataset.input_database == self['database'],
                ExchangeDataset.input_code == self['code'],
            ).execute()

        if databases[self['database']].get('searchable', True):
            IndexManager().delete_dataset(self)
            self._data['code'] = new_code
            IndexManager().add_datasets([self])
        else:
            self._data['code'] = new_code

        # Change _data['products'] as well

    def exchanges(self):
        return Exchanges(self.key)

    def technosphere(self):
        return Exchanges(
            self.key,
            kind="technosphere"
        )

    def biosphere(self):
        return Exchanges(
            self.key,
            kind="biosphere",
        )

    def production(self):
        return Exchanges(
            self.key,
            kind="production",
        )

    def upstream(self):
        return Exchanges(
            self.key,
            kind="technosphere",
            reverse=True
        )

    def new_exchange(self, **kwargs):
        """Create a new exchange linked to this activity"""
        exc = Exchange()
        exc.output = self.key
        for key in kwargs:
            exc[key] = kwargs[key]
        return exc

    @writable_project
    def copy(self, code=None, **kwargs):
        """Copy the activity. Returns a new `Activity`.

        `code` is the new activity code; if not given, a UUID is used.

        `kwargs` are additional new fields and field values, e.g. name='foo'

        """
        activity = Activity()
        for key, value in self.items():
            activity[key] = value
        for k, v in kwargs.items():
            setattr(activity._data, k, v)
        activity._data[u'code'] = str(code or uuid.uuid4().hex)
        activity.save()

        for exc in self.exchanges():
            data = copy.deepcopy(exc._data)
            data['output'] = activity.key
            # Change `input` for production exchanges
            if exc['input'] == exc['output']:
                data['input'] = activity.key
            ExchangeDataset.create(**data)
        return activity


class Exchange(ExchangeProxyBase):
    def __init__(self, document=None):
        if document is None:
            self._document = ExchangeDataset()
            self._data = {}
        else:
            self._document = document
            self._data = self._document.data
            self._data['input'] = (self._document.input_database, self._document.input_code)
            self._data['output'] = (self._document.output_database, self._document.output_code)

    @writable_project
    def save(self):
        if not self.valid():
            raise ValidityError("This exchange can't be saved for the "
                "following reasons\n\t* " + \
                "\n\t* ".join(self.valid(why=True)[1])
            )

        databases.set_dirty(self['output'][0])

        for key, value in dict_as_exchangedataset(self._data).items():
            setattr(self._document, key, value)
        self._document.save()

    @writable_project
    def delete(self):
        self._document.delete_instance()
        databases.set_dirty(self['output'][0])
        self = None
