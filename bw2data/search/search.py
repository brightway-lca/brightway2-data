from .indices import IndexManager
from whoosh.qparser import MultifieldParser
from whoosh.query import Term, And


def keysplit(strng):
    """Split an activity key joined into a single string using the magic sequence `⊡|⊡`"""
    return tuple(strng.split("⊡|⊡"))


class Searcher(object):
    def __init__(self, database):
        self._database = database

    def __enter__(self):
        self.index = IndexManager(self._database).get()
        return self

    def __exit__(self, type, value, traceback):
        self.index.close()

    def search(
        self,
        string,
        limit=25,
        facet=None,
        proxy=True,
        boosts=None,
        filter=None,
        mask=None,
    ):
        from .. import get_activity

        lowercase = lambda x: x.lower() if hasattr(x, "lower") else x
        string = lowercase(string)

        fields = [
            "name",
            "comment",
            "product",
            "categories",
            "synonyms",
            "location",
        ]

        boosts = boosts or {
            "name": 5,
            "comment": 1,
            "product": 3,
            "categories": 2,
            "synonyms": 3,
            "location": 3,
        }

        qp = MultifieldParser(fields, self.index.schema, fieldboosts=boosts)

        kwargs = {"limit": limit}
        if filter is not None:
            assert isinstance(filter, dict), "`filter` must be a dictionary"
            for k in filter:
                assert k in fields, "`filter` field {} not in search schema".format(k)
            if len(filter) == 1:
                kwargs["filter"] = [Term(k, lowercase(v)) for k, v in filter.items()][0]
            else:
                kwargs["filter"] = And(
                    [Term(k, lowercase(v)) for k, v in filter.items()]
                )
        if mask is not None:
            assert isinstance(mask, dict), "`mask` must be a dictionary"
            for k in mask:
                assert k in fields, "`mask` field {} not in search schema".format(k)
            if len(mask) == 1:
                kwargs["mask"] = [Term(k, lowercase(v)) for k, v in mask.items()][0]
            else:
                kwargs["mask"] = And([Term(k, lowercase(v)) for k, v in mask.items()])

        with self.index.searcher() as searcher:
            if facet is None:
                results = searcher.search(qp.parse(string), **kwargs)
                if "mask" in kwargs or "filter" in kwargs:
                    print(
                        "Excluding {} filtered results".format(results.filtered_count)
                    )
                results = [dict(obj.items()) for obj in results]
            else:
                kwargs.pop("limit")
                results = {
                    k: [searcher.stored_fields(i) for i in v]
                    for k, v in searcher.search(
                        qp.parse(string), groupedby=facet, **kwargs
                    )
                    .groups()
                    .items()
                }

        if proxy and facet is not None:
            return {
                key: [get_activity((obj["database"], obj["code"])) for obj in value]
                for key, value in results.items()
            }
        elif proxy:
            return [get_activity((obj["database"], obj["code"])) for obj in results]
        else:
            return results
