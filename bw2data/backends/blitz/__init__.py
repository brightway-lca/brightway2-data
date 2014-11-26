from bw2data import config
from bw2data.backends.base import LCIBackend
import blitzdb
import os


class Dataset(blitzdb.Document):
    pass


class Exchange(blitzdb.Document):
    pass


lci_backend = blitzdb.FileBackend(
    os.path.join(config.request_dir("blitz"), "lci"),
    {'serializer_class': 'pickle'}
)

def create_indices():
    lci_backend.create_index(Dataset, "code")
    lci_backend.create_index(Dataset, "database")
    lci_backend.create_index(Dataset, "name")
    lci_backend.create_index(Exchange, "input")


class BlitzLCIDatabase(LCIBackend):
    backend = "blitzdb"

    def load(self):
        pass

    def write(self, data):
        pass


config.backends['blitzdb'] = BlitzLCIDatabase


# Testing for Ecoinvent

# from brightway2 import *
# from copy import deepcopy

# lci_backend.begin()

# for index, (key, ds) in enumerate(Database("ecoinvent 2.2").load().items()):
#     if index and not index % 100:
#         print index
#         lci_backend.commit()
#         lci_backend.begin()

#     ds = deepcopy(ds)
#     ds['code'] = key[1]
#     ds['database'] = key[0]
#     for exc in ds['exchanges']:
#         exc = deepcopy(exc)
#         Exchange(exc).save(lci_backend)
#     del ds['exchanges']
#     Dataset(ds).save(lci_backend)

# lci_backend.commit()
