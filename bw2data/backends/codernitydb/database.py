# from . import lci_database_backend
# from ...utils import recursive_str_to_unicode
# from ..base import LCIBackend
# from copy import deepcopy
# import random
from . import cdb_datasets, cdb_exchanges


class CodernityDBBackend(object):
    def write(self, data):
        """Write a set of datasets to the database.

        .. note:: This is not 100% identical to normal writes. It does not delete datasets not in ``data``.

        """
        if data is not None:
            lci_database_backend.begin()
            for index, (key, ds) in enumerate(data.iteritems()):
                ds = recursive_str_to_unicode(deepcopy(ds))
                ds[u'database'], ds[u'code'] = key
                self.add(ds, False)
                if index and not index % 1000:
                    lci_database_backend.commit()
                    lci_database_backend.begin()
            lci_database_backend.commit()
        lci_database_backend.commit()



# db = CDatabase('/tmp/cdrnty')
# try:
#     db.create()
# except:
#     db.open()

# from time import time
# start = time()
# ei_data = Database("ecoinvent 3.1 cutoff", "singlefile").load()
# print "Loaded database data:", time() - start

# start = time()
# for key, ds in ei_data.iteritems():
#     ds[u'database'] = key[0]
#     ds[u'code'] = key[1]
#     del ds[u'exchanges']
#     codernity_db.insert(ds)
# print "Insert into cdrnty:", time() - start
