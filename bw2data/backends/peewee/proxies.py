from ... import databases
from ...errors import ValidityError
from ...proxies import ActivityProxyBase, ExchangeProxyBase
from ...search import IndexManager
from ...utils import get_activity
from .schema import ActivityDataset, ExchangeDataset
from .utils import dict_as_activity, keyjoin
import copy
import collections
import datetime
import warnings
import uuid


class Exchanges(collections.Iterable):
    """Iterator for exchanges with some additional methods"""
    def __init__(self, key, kind=None, raw=False, reverse=False):
        self._key = key
        self._kind = kind
        self._raw = raw
        if reverse:
            self._args = [ExchangeDataset.input == self._key]
        else:
            self._args = [ExchangeDataset.output == self._key]
        if self._kind:
            self._args.append(ExchangeDataset.type == self._kind)

    def filter(self, expr):
        self._args.append(expr)

    def count(self):
        return len(self)

    def delete(self):
        ExchangeDataset.delete().where(*self._args).execute()

    def _get_queryset(self):
        return ExchangeDataset.select().where(*self._args)

    def __iter__(self):
        for obj in self._get_queryset():
            if self._raw:
                yield obj
            else:
                yield Exchange(obj)

    def __len__(self):
        return self._get_queryset().count()


class Activity(ActivityProxyBase):
    def __init__(self, document=None):
        self._document = document or ActivityDataset()
        self._data = self._document.data if document else {}

    @property
    def key(self):
        return (self.database, self.code)

    def delete(self):
        self._document.delete_instance()
        self.exchanges().delete()
        self = None
        return None

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
        return Exchanges(self._document.key, raw=raw)

    def technosphere(self, raw=False):
        return Exchanges(self._document.key, kind=u"technosphere", raw=raw)

    def biosphere(self, raw=False):
        return Exchanges(self._document.key, kind=u"biosphere", raw=raw)

    def upstream(self, raw=False):
        return Exchanges(self._document.key, kind=u"technosphere",
                         raw=raw, reverse=True)

    def new_exchange(self, **kwargs):
        """Create a new exchange linked to this activity"""
        exc = Exchange()
        exc.output = self
        return exc

    def copy(self, name, code=None):
        activity = Activity()
        for key, value in self.items():
            activity[key] = value
        activity[u'database'] = self.database
        activity[u'code'] = unicode(code or uuid.uuid4().hex)
        activity[u'name'] = unicode(name)
        activity.save()

        for exc in self.exchanges():
            data = copy.deepcopy(exc._data)
            data['output'] = activity.key
            new_data = {
                u'data': data,
                u'type': exc.type,
                u'output': keyjoin(activity.key),
                u'input': exc._document.input,
                u'database': self.database
            }
            ExchangeDataset.create(**new_data)
        return activity


class Exchange(ExchangeProxyBase):
    def __init__(self, document=None):
        if document is None:
            self._document = ExchangeDataset()
            self._data = {}
            self.input = u"Unknown"
            self.output = u"Unknown"
        else:
            self._document = document
            self.input = get_activity(document.data['input'])
            self.output = get_activity(document.data['output'])
            self._data = self._document.data

    def save(self):
        for field in [u"database", u"type"]:
            setattr(self._document, field, self._data[field])
        self._document.data = self._data
        self._document.input = keyjoin(self._data['input'])
        self._document.output = keyjoin(self._data['output'])
        self._document.save()

    def __setitem__(self, key, value):
        pass
        # Warn if activity doesn't exist
