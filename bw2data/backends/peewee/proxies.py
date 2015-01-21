from ... import databases
from ...errors import ValidityError
from ...revisions import RevisionsInterface
from .schema import ActivityDataset, ExchangeDataset
from .utils import dict_as_activity, keyjoin
import collections
import datetime


class ProxyBase(collections.MutableMapping):
    def as_dict(self):
        return self._data

    def __str__(self):
        return unicode(self).encode('utf-8')

    __repr__ = __str__

    def __contains__(self, key):
        return key in self._data

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __delitem__(self, key):
        del self._data[key]

    def __getattr__(self, attr):
        try:
            return self._data[attr]
        except KeyError:
            return None

    def __eq__(self, other):
        return self._dict == other

    def __hash__(self):
        return hash(self._dict)

    def __setattr__(self, attr, value):
        if hasattr(self, "_data") and attr in self._data:
            raise AttributeError(u"Use `foo['bar'] = 'baz'` instead of "
                                 u"`foo.bar = 'baz'` for setting values.")
        else:
            super(ProxyBase, self).__setattr__(attr, value)


class Activity(ProxyBase):
    def __init__(self, document=None):
        self._document = document or ActivityDataset()
        self._data = self._document.data if document else {}

    def save(self):
        if not self.valid():
            raise ValidityError(u"This activity can't be saved for the "
                u"following reasons\n\t* " + \
                u"\n\t* ".join(self.valid(why=True)[1])
            )

        databases[self.database]['modified'] = datetime.datetime.now().isoformat()
        databases.flush()

        RevisionsInterface.save(self)
        as_activity = dict_as_activity(self._data)
        for field in [u"database", u"location", u"product", u"name", u"key"]:
            setattr(self._document, field, as_activity[field])
        self._document.data = self._data
        self._document.save()

    def valid(self, why=False):
        errors = []
        if u"database" not in self._data:
            errors.append(u"Missing field ``database``")
        elif self.database not in databases:
            errors.append(u"``database`` refers to unknown database")
        if u"code" not in self._data:
            errors.append(u"Missing field ``code``")
        if u"name" not in self._data:
            errors.append(u"Missing field ``name``")
        if errors:
            if why:
                return (False, errors)
            else:
                return False
        else:
            return True

    @property
    def key(self):
        return (self.database, self.code)

    @property
    def dbkey(self):
        return keyjoin(self.key)

    @property
    def revisions(self):
        return RevisionsInterface.revisions(self)

    def revert(self, index):
        revision = RevisionsInterface.revert(self, index)
        self._data = revision.data

    def __unicode__(self):
        if self.valid():
            return u"'%s' (%s, %s, %s)" % (self.name, self.unit, self.location,
                                           self.categories)
        else:
            return u"Activity with missing fields (call ``valid(why=True)`` to see more)"

    def __eq__(self, other):
        return self.key == other

    def __hash__(self):
        return hash(self.key)

    def __getitem__(self, key):
        # Basically a hack to let this act like a tuple with two
        # elements, database and code.
        if key == 0:
            return self.database
        elif key == 1:
            return self.code
        return self._data[key]

    def __delitem__(self, key):
        if key in {u"database", u"code"}:
            raise ValueError(u"Activities must have `database` and `code` values.")
        else:
            del self._data[key]

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


class Exchange(ProxyBase):
    def __init__(self, document=None):
        self._document = document or ExchangeDataset()
        self._data = self._document.data if document else {}

    def save(self):
        as_activity = dict_as_activity(self._data)
        for field in [u"database", u"location", u"product", u"name", u"key"]:
            setattr(self._document, field, as_activity[field])
        self._document.save()

    def __unicode__(self):
        return u"Exchange"

    def __repr__(self):
        return (u"<Exchange proxy for {}:{}>".format(self.input, self.output)).encode('utf8')
