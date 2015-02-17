import collections
from .. import databases


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


class ActivityProxyBase(ProxyBase):
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

    def valid(self, why=False):
        errors = []
        if not self.database:
            errors.append(u"Missing field ``database``")
        elif self.database not in databases:
            errors.append(u"``database`` refers to unknown database")
        if not self.code:
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
