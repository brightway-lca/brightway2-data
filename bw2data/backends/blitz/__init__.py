from ... import config
import blitzdb
import os

lci_database_backend = blitzdb.FileBackend(
    os.path.join(config.request_dir("blitz"), "lci"),
    {'serializer_class': 'pickle'}
)


def create_indices():
    lci_database_backend.create_index(Dataset, "code")
    lci_database_backend.create_index(Dataset, "database")
    lci_database_backend.create_index(Dataset, "name")
    lci_database_backend.create_index(Exchange, "input")

if not len(lci_database_backend.indexes) > 1:
    create_indices()


from .documents import ActivityDocument, ExchangeDocument
from .database import BlitzLCIDatabase
