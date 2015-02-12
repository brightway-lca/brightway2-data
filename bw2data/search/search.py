from .indices import IndexManager
from whoosh.qparser import MultifieldParser
from whoosh.collectors import TimeLimitCollector


class Searcher(object):
    def __init__(self):
        self.index = IndexManager().get()

    def search(self, string, limit=25, facet=None, proxy=False, sortedby=None, time_limit=2.):
        fields = [u"name", u"comment", u"product", u"categories"]
        qp = MultifieldParser(
            fields,
            self.index.schema,
            fieldboosts={u"name": 5., u"categories": 2., u"product": 3.}
        )
        with self.index.searcher() as searcher:
            if timelimit:
                _clctr = searcher.collector(limit=limit, sortedby=sortedby)
                collector = TimeLimitedCollector(_clctr, timelimit=timelimit)
            else:
                collector = searcher.collector(limit=limit, sortedby=sortedby)

            if facet is None:
                results = [
                    dict(obj.iteritems())
                    for obj in searcher.search_with_collector(qp.parse(string),
                        collector, limit=limit
                    )
                ]
            else:
                results = {
                    k: [searcher.stored_fields(i) for i in v] for k, v in
                    searcher.search_with_collector(qp.parse(string), collector,
                        groupedby=facet).groups().iteritems()
                }
        return results
