# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *
from future.utils import python_2_unicode_compatible

from .data_store import ProcessedDataStore
from .utils import safe_filename
import hashlib
import string


def abbreviate(names, length=8):
    """Take a tuple or list, and construct a string, doing the following:

    First, apply :func:`.utils.safe_filename` to each element in ``names``.

    Next, take the following, in order:
        * The first word of the first element in names, lower-cased, where word is defined as everything up to the first empty space character.
        * Join the rest of the first element (i.e. after the first word) with all other elements. Use the empty space character to join.
        * In this long string separated by spaces, take the lowercase first character of each word. Add the first word to this new string.
        * Finally, add a dash, and then the MD5 hash of the entire identifier, where each element is joined by a dash character.

    ``('ReCiPe Endpoint (E,A)', 'human health', 'ionising radiation')`` becomes ``'recipee(hhir-70eeef20a20deb6347ad428e3f6c5f3c'``.

    The MD5 hash is needed because taking the first characters doesn't guarantee unique strings.

    """
    safe_names = [safe_filename(x, False) for x in names]
    abbrev = lambda x: x if x[0] in string.digits else x[0].lower()
    name = " ".join(safe_names).split(" ")[0].lower() + \
        "".join([abbrev(x) for x in " ".join(safe_names).split(" ")[1:]])
    return name + "." + str(
        hashlib.md5(
            ("-".join(names)).encode('utf-8')
        ).hexdigest()
    )


@python_2_unicode_compatible
class ImpactAssessmentDataStore(ProcessedDataStore):
    """
A subclass of ``DataStore`` for impact assessment methods.

IA objects are hierarchically structured, and their identifier uses this structure, like ``('ecological scarcity 2006', 'total', 'natural resources')``. The identifier must be a ``tuple``, i.e. ``()``, not a ``list``, i.e. ``[]``. The identifier should only contain unicode strings, and can be of any length >= 1.

Because impact assessment methods are identified by a tuple of strings, e.g. ``('ReCiPe Endpoint (E,A)', 'human health', 'ionising radiation')``, we need to transform this identifier before it can be used e.g. as a filename. We do this using the :func:`.abbreviate` function, which returns a single unicode string.

Args:
    * *name* (tuple): Name of the IA object to manage. Must be a tuple of unicode strings.

    """
    def __str__(self):
        return "Brightway2 %s: %s" % (
            self.__class__.__name__,
            ": ".join(self.name)
        )

    def get_abbreviation(self):
        """Retrieve the abbreviation of the method identifier from the metadata store. See class documentation."""
        self.register()
        return self.metadata["abbreviation"]

    def copy(self, name=None):
        """Make a copy of the method, including its CFs and metadata.

        If ``name`` is not provided, add "Copy of" to the last element of the original name, e.g. ``("foo", "bar")`` becomes ``("foo", "Copy of bar")``

        Args:
            * *name* (tuple, optional): Name of the new method.

        Returns:
            The new object.

        """
        if name is None:
            name = self.name[:-1] + ("Copy of " + self.name[-1],)
        else:
            name = tuple(name)

        return super(ImpactAssessmentDataStore, self).copy(name)

    def register(self, **kwargs):
        """Register an object with the metadata store.

        The metadata key ``abbreviation`` is set automatically.

        Objects must be registered before data can be written. If this object is not yet registered in the metadata store, a warning is written to **stdout**.

        Takes any number of keyword arguments.

        """
        kwargs['abbreviation'] = abbreviate(self.name)
        super(ImpactAssessmentDataStore, self).register(**kwargs)

    @property
    def filename(self):
        return self.get_abbreviation()
