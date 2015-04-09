# -*- coding: utf-8 -*
from . import ActivityProxyBase
from .exchange import Exchange


class Activity(ActivityProxyBase):
    def __init__(self, key, database=None, data=None):
        from .. import Database
        self.key = key
        self._database = database or Database(self.key[0])
        self._data = data or self._database.get(self.key[1])

    def __setitem__(self, key, value):
        raise AttributeError("Activity proxies are read-only.")

    @property
    def database(self):
        return self.key[0]

    @property
    def code(self):
        return self.key[1]

    def save(self):
        raise NotImplemented

    def exchanges(self, raw=False):
        return [exc if raw else Exchange(exc, self)
                for exc in self._data.get(u'exchanges', [])]

    def technosphere(self, raw=False):
        return [exc if raw else Exchange(exc, self)
                for exc in self._data.get(u'exchanges', [])
                if exc.get('type') == 'technosphere']

    def biosphere(self, raw=False):
        return [exc if raw else Exchange(exc, self)
                for exc in self._data.get(u'exchanges', [])
                if exc.get('type') == 'biosphere']

    def upstream(self, *args, **kwargs):
        raise NotImplemented
