# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ...proxies import ActivityProxyBase
from ..single_file import Exchange


class Activity(ActivityProxyBase):
    def __init__(self, key, data={}):
        data["database"], data["code"] = key[0], key[1]
        self._data = data

    def __setitem__(self, key, value):
        raise AttributeError("Activity proxies are read-only.")

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
