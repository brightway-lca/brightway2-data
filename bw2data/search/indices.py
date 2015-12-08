# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .. import config, projects
from ..sqlite import keyjoin
from .schema import bw2_schema
from whoosh import index as windex
from whoosh import query
import os


path = projects.request_directory(u"whoosh")
if not os.path.exists(path):
    os.mkdir(path)

try:
    index_ = windex.open_dir(path)
except index.EmptyIndexError:
    index_ = windex.create_in(path, bw2_schema)


class IndexManager(object):
    def __init__(self, dir_name=u"whoosh"):
        self.path = projects.request_directory(u"whoosh")

    def get(self):
        return index_
        # try:
        #     return index.open_dir(self.path)
        # except index.EmptyIndexError:
        #     return self.create()

    def create(self):
        return
        return windex.create_in(self.path, bw2_schema)

    def reset(self):
        return self.create()

    def _format_dataset(self, ds):
        return dict(
            name=ds.get(u"name", u""),
            comment=ds.get(u"comment", u""),
            product=ds.get(u"reference product", u""),
            categories=u", ".join(ds.get(u"categories", [])),
            location=ds.get(u"location", u""),
            database=ds[u"database"],
            key=keyjoin((ds[u'database'], ds[u'code']))
        )

    def add_dataset(self, ds):
        writer = self.get().writer()
        writer.add_document(**self._format_dataset(ds))
        writer.commit()

    def add_datasets(self, datasets):
        writer = self.get().writer()
        for ds in datasets:
            writer.add_document(**self._format_dataset(ds))
        writer.commit()

    def update_dataset(self, ds):
        writer = self.get().writer()
        writer.update_document(**self._format_dataset(ds))
        writer.commit()

    def delete_dataset(self, ds):
        index = self.get()
        index.delete_by_term(u"key", keyjoin((ds[u'database'], ds[u'code'])))

    def delete_database(self, db_name):
        index = self.get()
        index.delete_by_query(query.Term("database", db_name))
