from ..proxies import Activity, Exchange
# from .storage import Storage
import itertools
from collections.abc import Iterable, Mapping
import pandas as pd
from types import GeneratorType
from warnings import warn


class ReadOnlyExchange(Mapping):
    # Helper class, which imitates behavior of bw2data.proxies.Exchange, except it does not inherit write methods

    __str__ = Exchange.__str__
    __lt__ = Exchange.__lt__
    __repr__ = Exchange.__repr__
    __contains__ = Exchange.__contains__
    __iter__ = Exchange.__iter__
    __len__ = Exchange.__len__
    __getitem__ = Exchange.__getitem__
    __eq__ = Exchange.__eq__
    __hash__ = Exchange.__hash__
    _get_input = Exchange._get_input
    _get_output = Exchange._get_output
    input = property(_get_input)
    output = property(_get_output)
    valid = Exchange.valid
    unit = Exchange.unit
    amount = Exchange.amount
    lca = Exchange.lca
    as_dict = Exchange.as_dict

    def __init__(self, **kwargs):
        self._data = kwargs


class IOTableExchanges(Iterable):
    def __init__(self, data):
        self.data = data

    def __iter__(self):
        return self.data.__iter__()

    def __next__(self):
        return self.data.__next__()

    def __len__(self):
        if isinstance(self.data, GeneratorType) or isinstance(
            self.data, itertools.chain
        ):
            self.data = list(self.data)
        return self.data.__len__()

    def to_dataframe(self, ascending=False, fields=None):

        # default columns to include
        if fields is None:
            fields = [
                "name",
                "location/category/compartment",
                "amount",
                "unit",
                "exchange type",
                "reference product",
            ]

        # load exchange data
        df = pd.DataFrame(
            {
                "database": e["input"][0],
                "code": e["input"][1],
                "amount": e["amount"],
                "exchange type": e["type"],
            }
            for e in self.data
        ).set_index(["database", "code"])

        # load activity metadata
        df_meta = Storage.construct_or_load_metadata(
            df.index.get_level_values("database").unique()
        )

        # join both into one dataframe
        # sort values
        df = df.join(df_meta, how="left").sort_values("amount", ascending=ascending)
        # merge location, categories and compartments into one column
        df["location/category/compartment"] = df["location"]
        if "categories" in df:
            df["location/category/compartment"].fillna(df["categories"], inplace=True)
        if "compartment" in df:
            df["location/category/compartment"].fillna(df["compartment"], inplace=True)

        # filter fields and return
        return df[fields]


class IOTableActivity(Activity):
    # def __init__(self, act=None):
    #     if act is None:
    #         super().__init__()
    #     else:
    #         super().__init__(document=act._document)

    def delete(self):
        # TBD
        pass

    def technosphere(self):



        lca, rev_act_dict, _ = Storage.construct_or_load_matrices(self.key[0])

        # look up technosphere inputs
        col = lca.dicts.activity[self.key]
        (row_ids, _), t_vals = (
            lca.technosphere_matrix[:, col].nonzero(),
            lca.technosphere_matrix[:, col].data,
        )

        return IOTableExchanges(
            ReadOnlyExchange(input=t_in, output=self.key, amount=v, type="technosphere")
            for t_in, v in zip(rev_act_dict.loc[row_ids], t_vals)
            if t_in != self.key
        )

    def biosphere(self):

        lca, _, rev_bio_dict = Storage.construct_or_load_matrices(self.key[0])

        # look up biosphere inputs
        col = lca.dicts.activity[self.key]
        (row_ids, _), b_vals = (
            lca.biosphere_matrix[:, col].nonzero(),
            lca.biosphere_matrix[:, col].data,
        )

        return IOTableExchanges(
            ReadOnlyExchange(input=b_in, output=self.key, amount=v, type="biosphere")
            for b_in, v in zip(rev_bio_dict.loc[row_ids], b_vals)
        )

    def production(self):
        lca, _, _ = Storage.construct_or_load_matrices(self.key[0])

        # look up technosphere inputs
        col = lca.dicts.activity[self.key]
        val = lca.technosphere_matrix[col, col]
        return IOTableExchanges(
            [
                ReadOnlyExchange(
                    input=self.key, output=self.key, amount=val, type="production"
                )
            ]
        )

    def exchanges(self):
        return IOTableExchanges(
            itertools.chain(self.production(), self.technosphere(), self.biosphere())
        )

    def substitution(self):
        warn("IO Table doesn't store exchange types, only numeric data. All positive technosphere edges are `production`, all negative technosphere edges are `technosphere`. Returning an empty iterator.")
        return iter([])
