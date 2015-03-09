from .indices import IndexManager
from whoosh.collectors import TimeLimitCollector, TimeLimit
from whoosh.qparser import MultifieldParser
from whoosh.query import Term, And


class Searcher(object):
    def __init__(self):
        self.index = IndexManager().get()

    def search(self, string, limit=25, facet=None, proxy=True, **kwargs):
        FILTER_TERMS = {u'name', u'product', u'location', u'database'}
        fields = [u"name", u"comment", u"product", u"categories"]

        filter_kwargs = {
            k: v for k, v in kwargs.items()
            if k in FILTER_TERMS
        }
        if len(filter_kwargs) > 1:
            And([Term(k, v) for k, v in filter_kwargs.items()])
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
                    dict(obj.iteritems())
                    for obj in searcher.search(qp.parse(string), limit=limit, filter=filter_kwargs)
                ]
            else:
                results = {
                    k: [searcher.stored_fields(i) for i in v] for k, v in
                    searcher.search(
                        qp.parse(string),
                        groupedby=facet,
                        filter=filter_kwargs
                    ).groups().iteritems()}

        if proxy and facet is not None:
            # TODO: Use get_activity
            return results
        elif proxy:
            return results
        else:
            return results
