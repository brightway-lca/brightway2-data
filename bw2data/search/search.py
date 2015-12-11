# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ..sqlite import keysplit
from .indices import IndexManager
from whoosh.collectors import TimeLimitCollector, TimeLimit
from whoosh.qparser import MultifieldParser
from whoosh.query import Term, And
import psutil


def open_files():
    proc = psutil.Process()
    return len(proc.open_files())


class Searcher(object):
    def __enter__(self):
        # print("Entering __enter__", open_files())
        self.index = IndexManager().get()
        # print("__enter__: Got index", open_files())
        return self

    def __exit__(self, type, value, traceback):
        # print("Calling __exit__", open_files())
        self.index.close()
        # print("__exit__: Closed index", open_files())

    def search(self, string, limit=25, facet=None, proxy=True, **kwargs):
        FILTER_TERMS = {u'name', u'product', u'location', u'database'}
        fields = [u"name", u"comment", u"product", u"categories"]

        filter_kwargs = {
            k: v for k, v in kwargs.items()
            if k in FILTER_TERMS
        }
        if len(filter_kwargs) > 1:
            filter_kwargs = And([Term(k, v) for k, v in filter_kwargs.items()])
        elif filter_kwargs:
            filter_kwargs = [Term(k, v) for k, v in filter_kwargs.items()][0]
        else:
            filter_kwargs = None

        qp = MultifieldParser(
            fields,
            self.index.schema,
            fieldboosts={u"name": 5., u"categories": 2., u"product": 3.}
        )

        with self.index.searcher() as searcher:
            if facet is None:
                results = [
                    dict(obj.items())
                    for obj in searcher.search(qp.parse(string), limit=limit, filter=filter_kwargs)
                ]
            else:
                results = {
                    k: [searcher.stored_fields(i) for i in v] for k, v in
                    searcher.search(
                        qp.parse(string),
                        groupedby=facet,
                        filter=filter_kwargs
                    ).groups().items()}

        from ..database import get_activity

        if proxy and facet is not None:
            return {key: [get_activity(keysplit(obj['key'])) for obj in value]
                    for key, value in results.items()}
        elif proxy:
            return [get_activity(keysplit(obj['key'])) for obj in results]
        else:
            return results
