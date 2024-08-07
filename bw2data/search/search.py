from itertools import groupby

import peewee

from .indices import IndexManager


def keysplit(strng):
    """Split an activity key joined into a single string using the magic sequence `⊡|⊡`"""
    return tuple(strng.split("⊡|⊡"))


class Searcher:
    search_fields = {
        "name",
        "comment",
        "product",
        "categories",
        "synonyms",
        "location",
    }

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
            node_class=None,
    ):
        from .. import get_activity

        lowercase = lambda x: x.lower() if hasattr(x, "lower") else x
        string = lowercase(string)

        boosts = boosts or {
            "name": 5,
            "comment": 1,
            "product": 3,
            "categories": 2,
            "synonyms": 3,
            "location": 3,
        }

        kwargs = {"limit": limit}
        if filter is not None:
            assert isinstance(filter, dict), "`filter` must be a dictionary"
            assert set(filter.keys()).issubset(self.search_fields), "`filter` fields {} not in search schema".format(
                set(filter.keys()).difference(self.search_fields))

            kwargs["filter"] = filter

        if mask is not None:
            assert isinstance(mask, dict), "`mask` must be a dictionary"
            assert set(mask.keys()).issubset(self.search_fields), "`mask` fields {} not in search schema".format(
                set(mask.keys()).difference(self.search_fields))

            kwargs["mask"] = mask
        if facet:
            kwargs.pop("limit")

        with self:
            try:
                results = self.index.search(string, weights=boosts, **kwargs)
            except peewee.OperationalError as e:
                if "no such table" in str(e):
                    results = None
                else:
                    raise

        if facet:
            results = {
                k: list(v)
                for k, v in groupby(results, lambda x: x.get(facet))
            }

        if proxy and facet is not None:
            return {
                key: [
                    get_activity(
                        key=(obj["database"], obj["code"]), node_class=node_class
                    )
                    for obj in value
                ]
                for key, value in results.items()
            }
        elif proxy:
            return [
                get_activity(key=(obj["database"], obj["code"]), node_class=node_class)
                for obj in results
            ]
        else:
            return results
