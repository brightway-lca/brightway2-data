from ..proxies import Activity, Exchange
from ...compat import prepare_lca_inputs
from bw2calc import LCA
import itertools
from collections.abc import Iterable
import pandas as pd


class IOTableExchanges(Iterable):
    def __init__(self, data):
        self.data = data

    def __iter__(self):
        return self.data.__iter__()

    def __next__(self):
        return self.data.__next__()

    def to_dataframe(self, ascending=True):
        return pd.DataFrame(
            [
                {
                    "input database": e.input.key[0],
                    "input name": e.input._data["name"],
                    "input location": e.input._data.get("location")
                    or e.input._data.get("categories"),
                    "input unit": e.input._data["unit"],
                    "amount": e.amount,
                    "type": e._data["type"],
                }
                for e in self.data
            ]
        ).sort_values("amount", ascending=ascending)


class IOTableActivity(Activity):
    def __init__(self, act=None):
        if act is None:
            super().__init__()
        else:
            super().__init__(document=act._document)

    def _construct_or_load_buffer(self):
        # load or construct helper LCA object containing technosphere and biosphere data

        from . import IOTableBackend

        # check if lca object has been constructed previously, load if yes
        db_name = self._data["database"]
        if (
            IOTableBackend.buffer is not None
            and IOTableBackend.buffer["db name"] == db_name
        ):
            lca = IOTableBackend.buffer["lca"]
            rev_act_dict = IOTableBackend.buffer["rev act dict"]
            rev_bio_dict = IOTableBackend.buffer["rev bio dict"]
        # otherwise construct new lca object and save in backend as class property
        else:
            if IOTableBackend.buffer is None:
                IOTableBackend.buffer = dict()
            demand, data_objs, remapping_dicts = prepare_lca_inputs({self.key: 1})
            lca = LCA(
                demand=demand, data_objs=data_objs, remapping_dicts=remapping_dicts
            )
            lca.load_lci_data()
            lca.remap_inventory_dicts()
            rev_act_dict = pd.Series(lca.dicts.activity.reversed)
            rev_bio_dict = pd.Series(lca.dicts.biosphere.reversed)
            IOTableBackend.buffer["db name"] = db_name
            IOTableBackend.buffer["lca"] = lca
            IOTableBackend.buffer["rev act dict"] = rev_act_dict
            IOTableBackend.buffer["rev bio dict"] = rev_bio_dict

        return lca, rev_act_dict, rev_bio_dict

    def technosphere(self, include_substitution=True):

        lca, rev_act_dict, _ = self._construct_or_load_buffer()

        # look up technosphere inputs
        col = lca.dicts.activity[self.key]
        (row_ids, _), t_vals = (
            lca.technosphere_matrix[:, col].nonzero(),
            lca.technosphere_matrix[:, col].data,
        )

        return IOTableExchanges(
            Exchange(input=t_in, output=self.key, amount=v, type="technosphere")
            for t_in, v in zip(rev_act_dict.loc[row_ids], t_vals)
            if t_in != self.key
        )

    def biosphere(self):

        lca, _, rev_bio_dict = self._construct_or_load_buffer()

        # look up biosphere inputs
        col = lca.dicts.activity[self.key]
        (row_ids, _), b_vals = (
            lca.biosphere_matrix[:, col].nonzero(),
            lca.biosphere_matrix[:, col].data,
        )

        return IOTableExchanges(
            Exchange(input=b_in, output=self.key, amount=v, type="biosphere")
            for b_in, v in zip(rev_bio_dict.loc[row_ids], b_vals)
        )

    def production(self):
        lca, _, _ = self._construct_or_load_buffer()

        # look up technosphere inputs
        col = lca.dicts.activity[self.key]
        val = lca.technosphere_matrix[col, col]
        return IOTableExchanges(
            [Exchange(input=self.key, output=self.key, amount=val, type="production")]
        )

    def exchanges(self):
        return IOTableExchanges(
            itertools.chain(self.production(), self.technosphere(), self.biosphere())
        )
