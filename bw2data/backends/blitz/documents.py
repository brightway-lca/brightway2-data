import blitzdb
from . import lci_database_backend


class Brightway2LCIDocument(blitzdb.Document):
    """Base class for LCI documents"""
    def __repr__(self):
        return unicode(self)


class ActivityDocument(Brightway2LCIDocument):
    def __unicode__(self):
        return u"{}: {} ({})".format(self.database, self.get('name') or 'No Name', self.code)

    def __hash__(self):
        return hash((self.database, self.code))

    # def save(self, *args, **kwargs):
    #     """Save an activity document.

    #     Call through Database object to get synchronization with search engine."""
    #     # self._check_valid()
    #     # print "Calling save!"
    #     # Do some stuff here
    #     # from . import BlitzLCIDatabase
    #     # BlitzLCIDatabase(self.database).delete(self)
    #     self.__save()

    def __save(self, backend):
        """Save dataset to blitzdb backend"""
        super(ActivityDocument, self).save(backend=backend)

    def delete(self, backend=None):
        """Delete an activity document.

        Call through Database object to get synchronization with search engine."""
        from . import BlitzLCIDatabase
        BlitzLCIDatabase(self.database).delete(self)

    def __delete(self, backend):
        """Delete dataset from blitzdb backend"""
        super(ActivityDocument, self).delete(backend=backend)

    class Meta(blitzdb.Document.Meta):
        primary_key = u"_pk"


class ExchangeDocument(Brightway2LCIDocument):
    def __init__(self, *args, **kwargs):
        kwargs[u'default_backend'] = lci_database_backend
        super(ExchangeDocument, self).__init__(*args, **kwargs)

    class Meta(blitzdb.Document.Meta):
        primary_key = u"_pk"
