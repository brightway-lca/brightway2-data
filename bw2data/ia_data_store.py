# -*- coding: utf-8 -*-
from .data_store import DataStore
from .utils import safe_filename
import hashlib
import string


def abbreviate(names, length=8):
    safe_names = [safe_filename(x, False) for x in names]
    abbrev = lambda x: x if x[0] in string.digits else x[0].lower()
    name = u" ".join(safe_names).split(" ")[0].lower() + \
        u"".join([abbrev(x) for x in u" ".join(safe_names).split(" ")[1:]])
    return name + u"." + hashlib.md5(unicode(u"-".join(names))).hexdigest()


class ImpactAssessmentDataStore(DataStore):
    """
A subclass of ``DataStore`` for impact assessment methods, which uses the ``abbreviate`` function to transform tuples of strings into a single string, and looks up abbreviations to generate filenames. Translated into less technical language, that means that we can't use ``('ReCiPe Endpoint (E,A)', 'human health', 'ionising radiation')`` as a filename, but we can use ``recipee(hhir-70eeef20a20deb6347ad428e3f6c5f3c``.

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
