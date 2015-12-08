# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ... import databases, mapping, geomapping, config
from ...errors import ValidityError, NotAllowed
from ...project import writable_project
from ...proxies import ActivityProxyBase, ExchangeProxyBase
from ...search import IndexManager
from ...sqlite import keyjoin, Key
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
            ExchangeDataset.input == Key(*self._key),
            # No production exchanges
            ExchangeDataset.output != Key(*self._key)
        ]
        else:
            self._args = [ExchangeDataset.output == Key(*self._key)]
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
        self._document = document or ActivityDataset()
        self._data = self._document.data if document else {}

    def __setitem__(self, key, value):
        if key in ('code', 'database') and key in self:
            raise NotAllowed(
                "Changing the `code` or `database` would break exchange links."
            )
        else:
            super(Activity, self).__setitem__(key, value)

    @property
    def key(self):
        return (self.get("database"), self.get("code"))

    @property
    def dbkey(self):
        return keyjoin(self.key)

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

    def exchanges(self):
        return Exchanges(self._document.key)

    def technosphere(self):
        return Exchanges(
            self._document.key,
            kind="technosphere"
        )

    def biosphere(self):
        return Exchanges(
            self._document.key,
            kind="biosphere",
        )

    def production(self):
        return Exchanges(
            self._document.key,
            kind="production",
        )

    def upstream(self):
        return Exchanges(
            self._document.key,
            kind="technosphere",
            reverse=True
        )

    def new_exchange(self, **kwargs):
        """Create a new exchange linked to this activity"""
        exc = Exchange()
        exc.output = self
        for key in kwargs:
            exc[key] = kwargs[key]
        return exc

    @writable_project
    def copy(self, name, code=None):
        activity = Activity()
        for key, value in self.items():
            activity[key] = value
        activity._data[u'database'] = self['database']
        activity._data[u'code'] = str(code or uuid.uuid4().hex)
        activity[u'name'] = str(name)
        activity.save()

        for exc in self.exchanges():
            data = copy.deepcopy(exc._data)
            data['output'] = activity.key
            new_data = {
                'data': data,
                'type': exc['type'],
                'output': activity.key,
                'input': exc._document.input,
                'database': self['database']
            }
            if exc['input'] == exc['output']:
                new_data['input'] = new_data['output']
            ExchangeDataset.create(**new_data)

        return activity


class Exchange(ExchangeProxyBase):
    def __init__(self, document=None):
        if document is None:
            self._document = ExchangeDataset()
            self._data = {}
        else:
            self._document = document
            self._data = self._document.data

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
