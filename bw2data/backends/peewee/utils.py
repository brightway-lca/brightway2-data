import cPickle as pickle


def dict_as_activity(ds):
    return {
        u"key": u":".join((ds[u"database"], ds[u"code"])),
        u"database": ds[u"database"],
        u"location": ds.get(u"location"),
        u"name": ds.get(u"name"),
        u"product": ds.get(u"reference product"),
        u"data": pickle.dumps(ds, protocol=pickle.HIGHEST_PROTOCOL)
    }
