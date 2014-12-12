from . import lci_database_backend
from ...utils import recursive_str_to_unicode
from ..base import LCIBackend
from .documents import ActivityDocument, ExchangeDocument
from .result_dict import ResultDict
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
    _update_cache = set()
    _delete_cache = set()
    _add_cache = set()

    # Add methods for save and delete
    # See:
    # http://whoosh.readthedocs.org/en/latest/indexing.html#updating-documents
    # http://whoosh.readthedocs.org/en/latest/indexing.html#deleting-documents
    # http://whoosh.readthedocs.org/en/latest/indexing.html#indexing-documents

    def write(self, data=None):
        """The `write` method is intended only for committing changes. Please add datasets manually.

        .. warning:: This method does not delete datasets not in ``data``.

        """
        if data is not None:
            lci_database_backend.begin()
            for index, (key, ds) in enumerate(data.iteritems()):
                ds = recursive_str_to_unicode(deepcopy(ds))
                ds[u'database'], ds[u'code'] = key
                self.add(ds, False)
                if index and not index % 1000:
                    lci_database_backend.commit()
                    lci_database_backend.begin()
            lci_database_backend.commit()
        lci_database_backend.commit()

    def add(self, ds, commit=True):
        def process_exchange(exc, ds):
            exc = recursive_str_to_unicode(deepcopy(exc))
            exc[u'output'] = (ds[u'database'], ds[u'code'])
            ed = ExchangeDocument(exc)
            ed.save()
            return ed

        assert u"database" and u"code" in ds, \
            u"New dataset must have `database` and `code` values"
        ad = ActivityDocument(ds)
        ad.exchanges = [process_exchange(exc, ds) for exc in ds.get(u'exchanges', [])]
        ad.save(lci_database_backend)
        if commit:
            lci_database_backend.commit()
        return ad

    def random(self):
        pk = random.choice(lci_database_backend.indexes['activitydocument'][
                             'code']._index.keys())
        return self.get(pk)

    def load(self, as_dict=False):
        def unroll_exchanges(ds):
            ds[u'exchanges'] = [obj.attributes for obj in ds.get(u'exchanges', [])]
            return ds

        if not as_dict:
            return ResultDict(lci_database_backend, self)
        else:
            return {(obj[u'database'], obj[u'code']): unroll_exchanges(obj)
                    for obj in self.filter()}

    def get(self, code):
        return lci_database_backend.get(
            ActivityDocument,
            {u'database': self.name, u'code': code}
        )

    def filter(**kwargs):
        kwargs[u'database'] = self.name
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

    def begin(self):
        """Begin a transaction in the blitz database backend.

        Not normally needed, for expert use only. Manually manually transactions means that your data isn't written to disk, even if you call ``save()`` on a dataset, until a ``commit_transaction()``.

        """
        lci_database_backend.begin()
        self._in_transaction = True

    def commit(self):
        """Finish a transaction in the blitz database backend.

        Not normally needed, called automatically each time an activity dataset is called unless transactions are manually managed.

        """

        lci_database_backend.commit()
        self._in_transaction = False
