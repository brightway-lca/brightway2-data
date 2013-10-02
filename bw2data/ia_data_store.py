# -*- coding: utf-8 -*-
from . import config
from copy import copy
from errors import UnknownObject, MissingIntermediateData
from utils import random_string
import os
import string
import warnings
try:
    import cPickle as pickle
except ImportError:
    import pickle


def abbreviate(names, length=8):
    abbrev = lambda x: x if x[0] in string.digits else x[0].lower()
    name = " ".join(names).split(" ")[0].lower() + \
        "".join([abbrev(x) for x in " ".join(names).split(" ")[1:]])
    return name + "-" + random_string(length)


class ImpactAssessmentDataStore(object):
    """
A manager for a impact assessment data. This class can register or deregister methods, write intermediate data, and copy methods.

This is meant to be subclassed, and should not be used directly.

Subclasses should define the following:

======== ========= ===========================================
name     type      description
======== ========= ===========================================
metadata attribute metadata class instances, e.g. ``methods``
label    attribute name for this kind of object, e.g. "method"
validate method    method that validates input data
process  method    method that writes processesd data to disk
======== ========= ===========================================

The ImpactAssessmentDataStore class never holds intermediate data, but it can load or write intermediate data. The only attribute is *name*, which is the name of the method being managed.

Instantiation does not load any data. If this IA object is not yet registered in the metadata store, a warning is written to ``stdout``.

IA objects are hierarchally structured, and this structure is preserved in the name. It is a tuple of strings, like ``('ecological scarcity 2006', 'total', 'natural resources')``.

Args:
    * *name* (tuple): Name of the IA object to manage. Must be a tuple of strings.

    """
    def __init__(self, name, *args, **kwargs):
        self.name = tuple(name)
        if self.name not in self.metadata and not \
                getattr(config, "dont_warn", False):
            warnings.warn("\n\t%s not a currently installed %s" % (
                " : ".join(self.name), self.label), UserWarning)

    def __unicode__(self):
        return u"%s: %s" % (self.label.title(), u"-".join(self.name))

    def __str__(self):
        return unicode(self).encode('utf-8')

    def get_abbreviation(self):
        """Abbreviate a method identifier (a tuple of long strings) for a filename. Random characters are added because some methods have similar names which would overlap when abbreviated."""
        try:
            return self.metadata[self.name]["abbreviation"]
        except KeyError:
            raise UnknownObject("This IA object is not yet registered")

    def copy(self, name=None):
        """Make a copy of the method.

        Args:
            * *name* (tuple, optional): Name of the new method.

        """
        name = tuple(name) or self.name[:-1] + ("Copy of " +
            self.name[-1],)
        new_object = self.__class__(name)
        metadata = copy(self.metadata[self.name])
        del metadata["abbreviation"]
        new_object.register(**metadata)
        new_object.write(self.load())

    def register(self, **kwargs):
        """Register a IA object with the metadata store.

        IA objects must be registered before data can be written.

        Takes any number of keyword arguments.

        """
        assert self.name not in self.metadata
        kwargs.update({"abbreviation": abbreviate(self.name)})
        self.metadata[self.name] = kwargs

    def deregister(self):
        """Remove an IA object from the metadata store. Does not delete any files."""
        del self.metadata[self.name]

    def write(self, data):
        """Serialize data to disk. Should be defined in each subclass.

        Args:
            * *data* (dict): Data

        """
        if self.name not in self.metadata:
            raise UnknownObject("This IA object is not yet registered")
        filepath = os.path.join(
            config.dir,
            "intermediate",
            "%s.pickle" % self.get_abbreviation()
        )
        with open(filepath, "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

    def load(self):
        """Load the intermediate data for this IA object.

        Returns:
            The intermediate data, a dictionary.

        """
        try:
            return pickle.load(open(os.path.join(
                config.dir,
                "intermediate",
                "%s.pickle" % self.get_abbreviation()
            ), "rb"))
        except OSError:
            raise MissingIntermediateData("Can't load intermediate data")

    def process(self):
        raise NotImplemented("This must be defined separately for each class")

    def write_processed_array(self, array):
        """Base function to write processed NumPy arrays."""
        filepath = os.path.join(
            config.dir,
            "processed",
            "%s.pickle" % self.get_abbreviation()
        )
        with open(filepath, "wb") as f:
            pickle.dump(array, f, protocol=pickle.HIGHEST_PROTOCOL)
