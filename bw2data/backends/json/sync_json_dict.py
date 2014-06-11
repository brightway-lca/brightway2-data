from ...serialization import JsonWrapper
from ...utils import safe_filename
from .mapping import get_mapping
import collections
import os
import json


class SynchronousJSONDict(collections.MutableMapping):

    """A dictionary which stores each value as a separate file on disk. Values are loaded asynchronously, but saved synchronously.

    Dictionary keys are strings, and do not correspond with filenames. The utility function `safe_filename` is used to translate keys into allowable filenames, and a separate mapping dictionary is kept to map dictionary keys to filenames.

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
        if key not in self.mapping:
            raise KeyError
        if key not in self.cache:
            self.cache[key] = self.load_file(key)
        return self.cache[key]

    def __setitem__(self, key, value):
        assert isinstance(value, dict), "Can only store `dict`s as values"
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
