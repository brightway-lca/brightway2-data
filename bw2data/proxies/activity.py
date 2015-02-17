# -*- coding: utf-8 -*
from . import ActivityProxyBase


class Activity(ActivityProxyBase):
    def __init__(self, key, database=None, data=None):
        from .. import Database
        self.key = key
        self._database = database or Database(self.key[0])
        self._data = data or self._database.get(self.key[1])

    @property
    def exchanges(self):
        from .exchange import Exchange
        return [Exchange(exc) for exc in self.raw.get(u'exchanges', [])]

    @property
    def database(self):
        return self.key[0]

    @property
    def code(self):
        return self.key[1]

    def save(self):
        raise NotImplemented

    def lca(self, method=None, amount=1.):
        """Shortcut to construct an LCA object for this activity."""
        from bw2calc import LCA
        lca = LCA({self: amount}, method=method)
        lca.lci()
        if method is not None:
            lca.lcia()
        lca.fix_dictionaries()
        return lca
