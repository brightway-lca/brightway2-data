from ... import config
import blitzdb
import os

lci_database_backend = blitzdb.FileBackend(
    os.path.join(config.request_dir("blitz"), "lci"),
    {'serializer_class': 'pickle'}
)

from .database import BlitzLCIDatabase
from .documents import ActivityDocument, ExchangeDocument
from .result_dict import ResultDict


def create_indices():
    lci_database_backend.create_index(ActivityDocument, "code")
    lci_database_backend.create_index(ActivityDocument, "database")
    lci_database_backend.create_index(ActivityDocument, "name")
    lci_database_backend.create_index(ExchangeDocument, "input")
    lci_database_backend.create_index(ExchangeDocument, "output")
    lci_database_backend.create_index(ExchangeDocument, "input.0")
    lci_database_backend.create_index(ExchangeDocument, "output.0")

if not len(lci_database_backend.indexes) > 1:
    create_indices()
