from ...serialization import JsonWrapper
from ...utils import safe_filename
from .mapping import get_mapping
import collections
import os
import json


class frozendict(dict):
    """A dictionary that can be created but not modified.

    From http://code.activestate.com/recipes/414283-frozen-dictionaries/"""
    def _blocked_attribute(obj):
        raise AttributeError("A frozendict cannot be modified")
    _blocked_attribute = property(_blocked_attribute)

    __delitem__ = __setitem__ = clear = _blocked_attribute
    pop = popitem = setdefault = update = _blocked_attribute

    def __new__(cls, *args, **kw):
        new = dict.__new__(cls)
        dict.__init__(new, *args, **kw)
        return new

    def __init__(self, *args, **kw):
        pass


class SynchronousJSONDict(collections.MutableMapping):

    """A dictionary which stores each value as a separate file on disk. Values are loaded asynchronously, but saved synchronously.

    Dictionary keys are strings, and do not correspond with filenames. The utility function `safe_filename` is used to translate keys into allowable filenames, and a separate mapping dictionary is kept to map dictionary keys to filenames.

    Retrieving a key returns a ``frozendict``, which can't be modified. This is to make sure that all changes get synced to disk. To change a dataset you must replace it completely, i.e. this won't work (it will raise an ``AttributeError``):

    .. code-block:: python

        my_sync_dict['foo']['bar'] = 'baz'

    Instead, you must do:

    .. code-block:: python

        my_sync_dict['foo'] = {'bar': 'baz'}

    """

    def __init__(self, dirpath, dirname):
        self.dirpath = dirpath
        self.dirname = dirname
        self.mapping = get_mapping(dirpath)
        self.cache = {}

    def filepath(self, key):
        if key not in self.mapping:
            self.mapping[key] = safe_filename(key[1])
        return os.path.join(self.dirpath, self.mapping[key] + ".json")

    def save_file(self, key, data):
        # Use json instead of anyjson because need indent for version control
        with open(self.filepath(key), "w") as f:
            json.dump(data, f, indent=2)

    def load_file(self, key):
        return self.from_json(JsonWrapper.load(self.filepath(key)))

    def delete_file(self, key):
        os.remove(self.filepath(key))
        del self.mapping[key]

    def keys(self):
        return self.mapping.keys()

    def from_json(self, data):
        """Change exchange `inputs` from lists to tuples"""
        for exc in data.get(u"exchanges", []):
            exc[u"input"] = tuple(exc[u"input"])
        if u"key" in data:
            data[u"key"] = tuple(data[u"key"])
        return data

    def __getitem__(self, key):
        """Returns a frozendict to get synchronization right.

        If the user can modify ``my_dict['foo']['bar']``, then this doesn't call ``__setitem__`` for ``my_dict``, meaning changes don't get synced to disk."""
        if key not in self.mapping:
            raise KeyError
        if key not in self.cache:
            self.cache[key] = self.load_file(key)
        return frozendict(self.cache[key])

    def __setitem__(self, key, value):
        assert isinstance(value, dict), "Can only store `dict`s as values"
        value = dict(value)  # Unfreeze if necessary
        value[u"key"] = key
        self.cache[key] = value
        self.save_file(key, value)

    def __delitem__(self, key):
        if key not in self.mapping:
            raise KeyError
        if key in self.cache:
            del self.cache[key]
        self.delete_file(key)

    def __contains__(self, key):
        return key in self.mapping

    def __iter__(self):
        return iter(self.mapping)

    def __len__(self):
        return len(self.mapping)
