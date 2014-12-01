import blitzdb
from . import lci_database_backend


class ActivityDocument(blitzdb.Document):
    def __init__(self, *args, **kwargs):
        kwargs[u'default_backed'] = lci_database_backend
        super(ActivityDocument, self).__init__(self, *args, **kwargs)


class ExchangeDocument(blitzdb.Document):
    def __init__(self, *args, **kwargs):
        kwargs[u'default_backed'] = lci_database_backend
        super(ActivityDocument, self).__init__(self, *args, **kwargs)
