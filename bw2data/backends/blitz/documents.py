import blitzdb
from . import lci_database_backend


class Brightway2LCIDocument(blitzdb.Document):
    """Base class for LCI documents"""
    pass


class ActivityDocument(Brightway2LCIDocument):
    def __init__(self, *args, **kwargs):
        kwargs[u'default_backend'] = lci_database_backend
        super(ActivityDocument, self).__init__(*args, **kwargs)

    # TODO: as_dict

    class Meta(blitzdb.Document.Meta):
        primary_key = u"_pk"


class ExchangeDocument(Brightway2LCIDocument):
    def __init__(self, *args, **kwargs):
        kwargs[u'default_backend'] = lci_database_backend
        super(ExchangeDocument, self).__init__(*args, **kwargs)

    class Meta(blitzdb.Document.Meta):
        primary_key = u"_pk"
