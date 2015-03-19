from ... import databases
from ...errors import ValidityError
from ...proxies import ActivityProxyBase, ExchangeProxyBase
from ...search import IndexManager
from .schema import ActivityDataset, ExchangeDataset
from .utils import dict_as_activity, keyjoin
import datetime
import warnings


class Activity(ActivityProxyBase):
    def __init__(self, document=None):
        self._document = document or ActivityDataset()
        self._data = self._document.data if document else {}

    @property
    def key(self):
        return (self.database, self.code)

    def save(self):
        if not self.valid():
            raise ValidityError(u"This activity can't be saved for the "
                u"following reasons\n\t* " + \
                u"\n\t* ".join(self.valid(why=True)[1])
            )

        databases[self.database]['modified'] = datetime.datetime.now().isoformat()
        databases.flush()

        as_activity = dict_as_activity(self._data)
        for field in [u"database", u"location", u"product", u"name", u"key"]:
            setattr(self._document, field, as_activity[field])
        self._document.data = self._data
        self._document.save()

        if databases[self.database].get('searchable'):
            IndexManager().update_dataset(self._data)

    @property
    def dbkey(self):
        return keyjoin(self.key)

    def exchanges(self, raw=False):
        qs = ExchangeDataset.select().where(
            ExchangeDataset.output == self._document.key,
            ExchangeDataset.type == u"technosphere"
        )
        return (qs if raw else (Exchange(obj) for obj in qs))

    def biosphere(self, raw=False):
        qs = ExchangeDataset.select().where(
            ExchangeDataset.output == self._document.key,
            ExchangeDataset.type == u"biosphere"
        )
        return (qs if raw else (Exchange(obj) for obj in qs))

    def upstream(self, raw=False):
        qs =  ExchangeDataset.select().where(
            ExchangeDataset.output == self.dbkey
        )
        return (qs if raw else (Exchange(obj) for obj in qs))

    def new_exchange(self, **kwargs):
        """Create a new exchange linked to this activity"""
        exc = Exchange()
        exc.output = self
        return exc


class Exchange(ExchangeProxyBase):
    def __init__(self, document=None):
        from ..database import get_activity

        if document is None:
            self._document = ExchangeDataset()
            self._data = {}
            self.input = u"Unknown"
            self.output = u"Unknown"
        else:
            self._document = document or ExchangeDataset()
            self._data = self._document.data if document else {}
            self.input = get_activity(self._data['input'])
            self.output = get_activity(self._data['output'])


    def save(self):
        as_activity = dict_as_activity(self._data)
        for field in [u"database", u"location", u"product", u"name", u"key"]:
            setattr(self._document, field, as_activity[field])
        self._document.save()

    def __setitem__(self, key, value):
        pass
        # Warn if activity doesn't exist
