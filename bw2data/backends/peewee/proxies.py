from .utils import dict_as_activity
import collections
import cPickle as pickle
from .schema import ActivityDataset, ExchangeDataset


class ProxyBase(collections.MutableMapping):
    _data = {}

    def __init__(self, document):
        self._document = document
        self._data = pickle.loads(str(self._document.data))

    def as_dict(self):
        return self._data

    def __repr__(self):
        return unicode(self).encode('utf-8')

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
        if self._data and attr in self._data:
            raise AttributeError(u"Use `foo['bar'] = 'baz'` instead of "
                                 u"`foo.bar = 'baz'` for setting values.")
        else:
            super(ProxyBase, self).__setattr__(attr, value)


class Activity(ProxyBase):
    def save(self):
        as_activity = dict_as_activity(self._data)
        for field in [u"database", u"location", u"product", u"name", u"key"]:
            setattr(self._document, field, as_activity[field])
        self._document.save()

    @property
    def key(self):
        return (self.database, self.code)

    def __unicode__(self):
        return u"'%s' (%s, %s, %s)" % (self.name, self.unit, self.location,
                                       self.categories)

    def __str__(self):
        return str(self.key)

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

    @property
    def exchanges(self):
        return (Exchange(obj) for obj in ExchangeDataset.select().where(
                   ExchangeDataset.output == self._document.key,
                   ExchangeDataset.type_ == u"technosphere"
        ))

    @property
    def upstream(self):
        return (Exchange(obj) for obj in ExchangeDataset.select().where(ExchangeDataset.output == u":".join(self.key)))


class Exchange(ProxyBase):
    def __init__(self, document):
        self._document = document
        self._data = pickle.loads(str(self._document.data))

    def save(self):
        as_activity = dict_as_activity(self._data)
        for field in [u"database", u"location", u"product", u"name", u"key"]:
            setattr(self._document, field, as_activity[field])
        self._document.save()

    def __unicode__(self):
        return u"Exchange"

    def __str__(self):
        return "{:.3e} {}: {}".format(self.amount, self.input, self.output)
