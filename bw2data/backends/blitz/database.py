from . import lci_database_backend
from ...utils import recursive_str_to_unicode
from ..base import LCIBackend
from .documents import ActivityDocument
from copy import deepcopy
import random


class BlitzLCIDatabase(LCIBackend):
    """Database object for blitzdb backend."""
    def write(self, data=None):
        """The `write` method is intended only for committing changes. Please add datasets manually."""
        if data is not None:
            lci_database_backend.begin()
            for index, key, ds in enumerate(data.iteritems()):
                ds = deepcopy(ds)
                ds['database'], ds['code'] = key
                #     for exc in ds['exchanges']:
                #         exc = deepcopy(exc)
                #         Exchange(exc).save(lci_backend)
                #     del ds['exchanges']
                ActivityDocument(recursive_str_to_unicode(ds)).save()
                if index and not index % 100:
                    lci_database_backend.commit()
                    lci_database_backend.begin()
            lci_database_backend.commit()
        else:
            lci_database_backend.commit()

    def random(self):
        pass

    def get(self, code):
        return lci_database_backend.

    def filter(**kwargs):
        pass

