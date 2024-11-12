import bz2
import os
import pickle
import random
from collections.abc import MutableMapping
from copy import deepcopy
from pathlib import Path
from time import time
from typing import Union

from bw2data import projects
from bw2data.errors import PickleError
from bw2data.fatomic import open as atomic_open
from bw2data.utils import maybe_path

try:
    import anyjson
except ImportError:
    anyjson = None
    import json


class JsonWrapper:
    @classmethod
    def dump(self, data, filepath):
        with atomic_open(filepath, "w") as f:
            if anyjson:
                f.write(anyjson.serialize(data))
            else:
                json.dump(data, f, indent=2)

    @classmethod
    def dump_bz2(self, data, filepath):
        with atomic_open(filepath, "wb") as f:
            with bz2.BZ2File(f.name, "wb") as b:
                b.write((JsonWrapper.dumps(data)).encode("utf-8"))

    @classmethod
    def load(self, file):
        if anyjson:
            return anyjson.deserialize(open(file, encoding="utf-8").read())
        else:
            return json.load(open(file, encoding="utf-8"))

    @classmethod
    def load_bz2(self, filepath):
        return JsonWrapper.loads((bz2.BZ2File(filepath).read()).decode("utf-8"))

    @classmethod
    def dumps(self, data):
        if anyjson:
            return anyjson.serialize(data)
        else:
            return json.dumps(data)

    @classmethod
    def loads(self, data):
        if anyjson:
            return anyjson.deserialize(data)
        else:
            return json.loads(data)


class JsonSanitizer:
    @classmethod
    def sanitize(cls, data):
        if isinstance(data, tuple):
            return {"__tuple__": True, "data": [cls.sanitize(x) for x in data]}
        elif isinstance(data, dict):
            return {
                "__dict__": True,
                "keys": [cls.sanitize(x) for x in data.keys()],
                "values": [cls.sanitize(x) for x in data.values()],
            }
        elif isinstance(data, list):
            return [cls.sanitize(x) for x in data]
        else:
            return data

    @classmethod
    def load(cls, data):
        if isinstance(data, dict):
            if "__tuple__" in data:
                return tuple([cls.load(x) for x in data["data"]])
            elif "__dict__" in data:
                return dict(
                    zip(
                        [cls.load(x) for x in data["keys"]],
                        [cls.load(x) for x in data["values"]],
                    )
                )
            else:
                raise ValueError
        elif isinstance(data, list):
            return [cls.load(x) for x in data]
        else:
            return data


class SerializedDict(MutableMapping):
    """Base class for dictionary that can be `serialized <http://en.wikipedia.org/wiki/Serialization>`_ to or unserialized from disk. Uses JSON as its storage format. Has most of the methods of a dictionary.

    Upon instantiation, the serialized dictionary is read from disk."""

    def __init__(self, dirpath=None):
        if not getattr(self, "filename"):
            raise NotImplementedError(
                "SerializedDict must be subclassed, and the filename must be set."
            )
        self.filepath = (maybe_path(dirpath) or projects.dir) / self.filename
        self.load()

    def load(self):
        """Load the serialized data. Creates the file if not yet present."""
        try:
            self.data = self.deserialize()
        except IOError:
            # Create if not present
            self.data = {}
            # No need to send signal when there is no data
            self.flush(signal=False)

    def flush(self, signal: bool = True):
        """Serialize the current data to disk."""
        self.serialize(signal=signal)

    @property
    def list(self):
        """List the keys of the dictionary. This is a property, and does not need to be called."""
        return sorted(self.data.keys())

    def __getitem__(self, key):
        if isinstance(key, list):
            key = tuple(key)
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value
        self.flush()

    def __contains__(self, key):
        return key in self.data

    def __str__(self):
        if not len(self):
            return "{} dictionary with 0 objects".format(self.__class__.__name__)
        elif len(self) > 20:
            return (
                "{} dictionary with {} objects, including:"
                "{}\nUse `list(this object)` to get the complete list."
            ).format(
                self.__class__.__name__,
                len(self),
                "".join(["\n\t{}".format(x) for x in sorted(self.data)[:10]]),
            )
        else:
            return ("{} dictionary with {} object(s):{}").format(
                self.__class__.__name__,
                len(self),
                "".join(["\n\t{}".format(x) for x in sorted(self.data)]),
            )

    __repr__ = lambda x: str(x)

    def __delitem__(self, name: str, signal: bool = True):
        del self.data[name]
        self.flush(signal=signal)

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __hash__(self):
        return hash(self.data)

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def serialize(self, filepath: Union[str, Path] = None, signal: bool = True):
        """Method to do the actual serialization. Can be replaced with other serialization formats.

        Args:
            * *filepath* (str, optional): Provide an alternate filepath (e.g. for backup).

        """
        if signal and hasattr(self, "_save_signal"):
            try:
                previous = self.deserialize()
            except IOError:
                previous = {}

        with atomic_open(filepath or self.filepath, "w") as f:
            f.write(JsonWrapper.dumps(self.pack(self.data)))

        if signal and hasattr(self, "_save_signal"):
            self._save_signal.send(old=previous, new=deepcopy(self.data))

    def deserialize(self):
        """Load the serialized data. Can be replaced with other serialization formats."""
        return self.unpack(JsonWrapper.load(self.filepath))

    def pack(self, data):
        """Transform the data, if necessary. Needed because JSON must have strings as dictionary keys."""
        return data

    def unpack(self, data):
        """Return serialized data to true form."""
        return data

    def random(self):
        """Return a random key."""
        if not self.data:
            return None
        else:
            return random.choice(list(self.data.keys()))

    def backup(self):
        """Write a backup version of the data to the ``backups`` directory."""
        filepath = os.path.join(projects.dir, "backups", self.filename + ".%s.backup" % int(time()))
        self.serialize(filepath)


class PickledDict(SerializedDict):
    """Subclass of ``SerializedDict`` that uses the pickle format instead of JSON."""

    def serialize(self, signal: bool = True):
        if signal and hasattr(self, "_save_signal"):
            previous = self.deserialize()

        with atomic_open(self.filepath, "wb") as f:
            pickle.dump(self.pack(self.data), f, protocol=4)

        if signal and hasattr(self, "_save_signal"):
            self._save_signal.send(old=previous, new=deepcopy(self.data))

    def deserialize(self):
        try:
            return self.unpack(pickle.load(open(self.filepath, "rb")))
        except ImportError:
            TEXT = "Pickle deserialization error in file '%s'" % self.filepath
            raise PickleError(TEXT)


class CompoundJSONDict(SerializedDict):
    """Subclass of ``SerializedDict`` that allows tuples as dictionary keys (not allowed in JSON)."""

    def pack(self, data):
        """Transform the dictionary to a list because JSON can't handle lists as keys"""
        return [(k, v) for k, v in data.items()]

    def unpack(self, data):
        """Transform data back to a dictionary"""
        return dict([(tuple(x[0]), x[1]) for x in data])
