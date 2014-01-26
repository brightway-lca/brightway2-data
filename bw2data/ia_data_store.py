# -*- coding: utf-8 -*-
from . import config
from .data_store import DataStore
from copy import copy
from errors import UnknownObject, MissingIntermediateData
from utils import random_string
import hashlib
import os
import string
import warnings
try:
    import cPickle as pickle
except ImportError:
    import pickle


def abbreviate(names, length=8):
    abbrev = lambda x: x if x[0] in string.digits else x[0].lower()
    name = u" ".join(names).split(" ")[0].lower() + \
        u"".join([abbrev(x) for x in u" ".join(names).split(" ")[1:]])
    return name + u"-" + hashlib.md5(unicode(u"-".join(names))).hexdigest()


class ImpactAssessmentDataStore(DataStore):
    """
A subclass of ``DataStore`` for impact assessment methods, which uses the ``abbreviate`` function to transform tuples of strings into a single string, and looks up abbreviations to generate filenames.

A manager for a impact assessment data. This class can register or deregister methods, write intermediate data, and copy methods.

This is meant to be subclassed, and should not be used directly.

Subclasses should define the following:

======== ========= ===========================================
name     type      description
======== ========= ===========================================
metadata attribute metadata class instances, e.g. ``methods``
validate method    method that validates input data
process  method    method that writes processesd data to disk
======== ========= ===========================================

The ImpactAssessmentDataStore class never holds intermediate data, but it can load or write intermediate data. The only attribute is *name*, which is the name of the method being managed.

Instantiation does not load any data. If this IA object is not yet registered in the metadata store, a warning is written to ``stdout``.

IA objects are hierarchally structured, and this structure is preserved in the name. It is a tuple of strings, like ``('ecological scarcity 2006', 'total', 'natural resources')``.

Args:
    * *name* (tuple): Name of the IA object to manage. Must be a tuple of strings.

    """
    def __unicode__(self):
        return u"Brightway2 %s: %s" % (
            self.__class__.__name__,
            u": ".join(self.name)
        )

    def get_abbreviation(self):
        """Abbreviate a method identifier (a tuple of long strings) for a filename. Random characters are added because some methods have similar names which would overlap when abbreviated."""
        self.assert_registered()
        return self.metadata[self.name]["abbreviation"]

    def copy(self, name=None):
        """Make a copy of the method.

        Args:
            * *name* (tuple, optional): Name of the new method.

        """
        if name is None:
            name = self.name[:-1] + ("Copy of " + self.name[-1],)
        else:
            name = tuple(name)

        return super(ImpactAssessmentDataStore, self).copy(name)

    def register(self, **kwargs):
        """Register an object with the metadata store.

        Objects must be registered before data can be written. If this object is not yet registered in the metadata store, a warning is written to **stdout**.

        Takes any number of keyword arguments.

        """
        kwargs['abbreviation'] = abbreviate(self.name)
        super(ImpactAssessmentDataStore, self).register(**kwargs)

    @property
    def filename(self):
        return self.get_abbreviation()
