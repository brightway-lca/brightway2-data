import cPickle as pickle
import collections


class Activity(collections.MutableMapping):
    def __init__(self, document):
        self._document = document
        self._data = pickle.loads(str(self._document.data))
        self._dirty = False

    def save(self):
        if not self._dirty:
            return False
        self._document.data = pickle.dumps(
            self._data,
            protocol=pickle.HIGHEST_PROTOCOL
        )
        self._document.save(only=self._document.dirty_fields)
        self._dirty = False
        return True

    @property
    def key(self):
        return (self.database, self.code)

    def as_dict(self):
        return self._data

    def __unicode__(self):
        return u"'%s' (%s, %s, %s)" % (self.name, self.unit, self.location,
                                       self.categories)

    def __str__(self):
        return str((self.database, self.code))

    def __repr__(self):
        return unicode(self).encode('utf-8')

    def __eq__(self, other):
        return (self.database, self.code) == other

    def __hash__(self):
        return hash((self.database, self.code))

    def __contains__(self, key):
        return key in self._data

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __getitem__(self, key):
        if key == 0:
            return self.database
        elif key == 1:
            return self.code
        return self._data[key]

    def __setitem__(self, key, value):
        self._dirty = True
        if key == u"database":
            self._document.database = value
            self._document.key = u":".join((value, self.code))
        elif key == u"code":
            self._document.code = value
            self._document.key = u":".join((self.database, value))
        elif key == u"location":
            self._document.location = value
        elif key == u"name":
            self._document.name = value
        elif key == u"reference product":
            self._document.product = value
        self._data[key] = value

    def __delitem__(self, key):
        # TODO
        pass

    def __getattr__(self, attr):
        try:
            return self._data[attr]
        except KeyError:
            return None

    # TODO
    # def __setattr__(self, attr, value):
    #     if hasattr(self, attr):
    #         setattr(self, attr, value)
    #     else:
    #         raise ValueError(u"Use `foo['bar'] = 'baz'` instead of foo.bar"
    #                          u" = 'baz' for setting values.")
