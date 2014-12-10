from . import lci_database_backend
from ...utils import recursive_str_to_unicode
from ..base import LCIBackend
from .documents import ActivityDocument, ExchangeDocument
from blitzdb.backends.file.queryset import QuerySet
from copy import deepcopy
import random


class BlitzLCIDatabase(LCIBackend):
    """Database object for blitzdb backend.

    The API is the following:

    ``.process()`` works as normal.

    ``.write(data)`` does something... ``.commit()`` is done automatically.

    ``.commit()`` writes pending changes to the database.

    ``.get(code)`` returns an ``ActivityDocument``.

    ``.filter(key=value) returns a list of ``ActivityDocument``s.

    ``.load()`` returns a OnDemandDictionary.

    """
    _in_transaction = False

    def write(self, data=None):
        """The `write` method is intended only for committing changes. Please add datasets manually.

        .. warning:: This method does not delete datasets not in ``data``.

        """
        def process_exchange(exc, key):
            exc = recursive_str_to_unicode(deepcopy(exc))
            exc[u'output'] = key
            ed = ExchangeDocument(exc)
            ed.save()
            return ed

        if data is not None:
            lci_database_backend.begin()
            for index, (key, ds) in enumerate(data.iteritems()):
                ds = recursive_str_to_unicode(deepcopy(ds))
                ds[u'database'], ds[u'code'] = key
                ad = ActivityDocument(ds)
                ad.exchanges = [process_exchange(exc, key) for exc in ds.get(u'exchanges')]
                ad.save()
                if index and not index % 1000:
                    lci_database_backend.commit()
                    lci_database_backend.begin()
            lci_database_backend.commit()
        lci_database_backend.commit()

    def random(self):
        code = random.choice(lci_database_backend.indexes['activitydocument'][
                             'code']._index.keys())
        return self.get(code)

    def load(self, as_dict=False):
        if not as_dict:
            return lci_database_backend.filter(ActivityDocument, {})

    def get(self, code):
        return lci_database_backend.get(ActivityDocument, {u'code': code})

    def filter(**kwargs):
        return lci_database_backend.filter(ActivityDocument, kwargs)

    def exchanges(self):
        """Get QuerySet of all exchanges linked to by this database.

        .. note:: Does not include exchanges from other databases *into* this database.

        """
        return lci_database_backend.filter(ExchangeDocument, {'output.0': self.name})

    def query(self, *args, **kwargs):
        raise NotImplementedError

    def register(self, **kwargs):
        """Register a database with the metadata store, using the *blitz* backend."""
        kwargs[u"backend"] = u"blitz"
        super(BlitzLCIDatabase, self).register(**kwargs)

    def begin_transaction(self):
        """Begin a transaction in the blitz database backend.

        Not normally needed, for expert use only. Manually manually transactions means that your data isn't written to disk, even if you call ``save()`` on a dataset, until a ``commit_transaction()``.

        """
        lci_database_backend.begin()
        self._in_transaction = True

    def commit_transaction(self):
        """Finish a transaction in the blitz database backend.

        Not normally needed, called automatically each time an activity dataset is called unless transactions are manually managed.

        """
        lci_database_backend.commit()
        self._in_transaction = False
