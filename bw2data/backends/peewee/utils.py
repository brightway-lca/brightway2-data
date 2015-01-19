# -*- coding: utf-8 -*-
MAGIC_JOIN_CHARACTER = u"⊡"

def keyjoin(tpl):
    return MAGIC_JOIN_CHARACTER.join(tpl)


def keysplit(key):
    return tuple(key.split(MAGIC_JOIN_CHARACTER))


def dict_as_activity(ds):
    return {
        u"key": keyjoin((ds[u"database"], ds[u"code"])),
        u"database": ds[u"database"],
        u"location": ds.get(u"location"),
        u"name": ds.get(u"name"),
        u"product": ds.get(u"reference product"),
        u"data": ds
    }
