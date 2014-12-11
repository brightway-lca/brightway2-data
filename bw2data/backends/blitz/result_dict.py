import collections
from .documents import ActivityDocument


class ResultDict(collections.MutableMapping):
    """An object that acts like a dict. Returned by ``database.load()``."""
    def __init__(self, backend, database):
        self.backend = backend
        self.database = database  # ``Database`` object

    @property
    def qs(self):
        return self.backend.filter(ActivityDocument, {u'database': })

    def keys(self):
        for obj in self.qs:
            yield (obj.database, obj.code)

    def values(self):
        return self.qs

    def __getitem__(self, key):
        return self.database.get(key[1])

    def __setitem__(self, key, value):
        assert isinstance(value, dict), "Can only store `dict`s as new datasets"
        value[u"database"] = key[0]
        value[u"code"] = key[1]
        self.database.add(value)

    def __delitem__(self, key):
        self.database.delete(key[1])

    def __contains__(self, key):
        try:
            self.backend.get(
                ActivityDocument,
                {u'database': key[0], u'code': key[1]}
            )
            return True
        except ActivityDocument.DoesNotExist:
            return False

    def __iter__(self):
        return self.keys()

    def __len__(self):
        return len(self.qs)
