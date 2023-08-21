import collections


class Result:
    """A container that wraps a filtered dataset. Returned by a calling a ``Query`` object. A result object functions like a read-only dictionary; you can call ``Result[some_key]``, or ``some_key in Result``, or ``len(Result)``.

    The dataset can also be sorted, using ``sort(field)``; the underlying data is then a ``collections.OrderedDict``.

    Args:
        * *result* (dict): The filtered dataset.

    """

    def __init__(self, result):
        self.result = result
        if not isinstance(result, dict):
            raise ValueError("Must pass dictionary")

    def __str__(self):
        return "Query result with %i entries" % len(self.result)

    def __repr__(self):
        if not self.result:
            return "Query result:\n\tNo query results found."
        data = list(self.result.items())[:20]
        return "Query result: (total %i)\n" % len(self.result) + "\n".join(
            ["%s: %s" % (k, v.get("name", "Unknown")) for k, v in data]
        )

    def sort(self, field, reverse=False):
        """Sort the filtered dataset. Operates in place; does not return anything.

        Args:
            * *field* (str): The key used for sorting.
            * *reverse* (bool, optional): Reverse normal sorting order.

        """
        self.result = collections.OrderedDict(
            sorted(
                self.result.items(),
                key=lambda t: t[1].get(field, None),
                reverse=reverse,
            )
        )

    # Generic dictionary methods
    def __len__(self):
        return len(self.result)

    def __iter__(self):
        return iter(self.result)

    def keys(self):
        return self.result.keys()

    def items(self):
        return self.result.items()

    def items(self):
        return self.result.items()

    def __getitem__(self, key):
        return self.result[key]

    def __contains__(self, key):
        return key in self.result


class Query:
    """A container for a set of filters applied to a dataset.

    Filters are applied by calling the ``Query`` object, and passing the dataset to filter as the argument. Calling a ``Query`` with some data returns a ``Result`` object with the filtered dataset.

    Args:
        * *filters* (filters): One or more ``Filter`` objects.

    """

    def __init__(self, *filters):
        self.filters = list(filters)

    def add(self, filter_):
        """Add another filter.

        Args:
            *filter_* (``Filter``): A Filter object.

        """
        self.filters.append(filter_)

    def __call__(self, data):
        for filter_ in self.filters:
            data = filter_(data)
        return Result(data)
