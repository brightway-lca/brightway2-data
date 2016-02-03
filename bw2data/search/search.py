# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .indices import IndexManager
from whoosh.collectors import TimeLimitCollector, TimeLimit
from whoosh.qparser import MultifieldParser
from whoosh.query import Term, And
import psutil


def open_files():
    proc = psutil.Process()
    return len(proc.open_files())


def keysplit(strng):
    """Split an activity key joined into a single string using the magic sequence `⊡|⊡`"""
    return tuple(strng.split("⊡|⊡"))


class Searcher(object):
    def __init__(self, database):
        self._database = database

    def __enter__(self):
        # print("Entering __enter__", open_files())
        self.index = IndexManager(self._database).get()
        # print("__enter__: Got index", open_files())
        return self

    def __exit__(self, type, value, traceback):
        # print("Calling __exit__", open_files())
        self.index.close()
        # print("__exit__: Closed index", open_files())

    def search(self, string, limit=25, facet=None, proxy=True):
        fields = [u"name", u"comment", u"product", u"categories"]

        qp = MultifieldParser(
            fields,
            self.index.schema,
            fieldboosts={u"name": 5., u"categories": 2., u"product": 3.}
        )

        with self.index.searcher() as searcher:
            if facet is None:
                results = [
                    dict(obj.items())
                    for obj in searcher.search(qp.parse(string), limit=limit)
                ]
            else:
                results = {
                    k: [searcher.stored_fields(i) for i in v] for k, v in
                    searcher.search(
                        qp.parse(string),
                        groupedby=facet,
                    ).groups().items()}

        from ..database import get_activity

        if proxy and facet is not None:
            return {key: [get_activity((obj['database'], obj['code'])) for obj in value]
                    for key, value in results.items()}
        elif proxy:
            return [get_activity((obj['database'], obj['code'])) for obj in results]
        else:
            return results
