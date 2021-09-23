from ..proxies import Activity, Exchange
from ...utils import get_activity
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
            rev_act_dict = {v: k for k, v in lca.dicts.activity.items()}
            rev_bio_dict = {v: k for k, v in lca.dicts.biosphere.items()}
            IOTableBackend.buffer["db name"] = db_name
            IOTableBackend.buffer["lca"] = lca
            IOTableBackend.buffer["rev act dict"] = rev_act_dict
            IOTableBackend.buffer["rev bio dict"] = rev_bio_dict

        return lca, rev_act_dict, rev_bio_dict

    def technosphere(self, include_substitution=True):

        lca, rev_act_dict, rev_bio_dict = self._construct_or_load_buffer()

        # look up technosphere inputs
        col = lca.dicts.activity[self.id]
        (row_ids, _), t_vals = (
            lca.technosphere_matrix[:, col].nonzero(),
            lca.technosphere_matrix[:, col].data,
        )
        t_inputs = (get_activity(rev_act_dict[local_id]) for local_id in row_ids)

        return IOTableExchanges(
            Exchange(input=i.key, output=self.key, amount=v, type="technosphere")
            for i, v in zip(t_inputs, t_vals)
            if i.id != self.id
        )

    def biosphere(self):

        lca, rev_act_dict, rev_bio_dict = self._construct_or_load_buffer()

        # look up biosphere inputs
        col = lca.dicts.activity[self.id]
        (row_ids, _), b_vals = (
            lca.biosphere_matrix[:, col].nonzero(),
            lca.biosphere_matrix[:, col].data,
        )
        b_inputs = (get_activity(rev_bio_dict[local_id]) for local_id in row_ids)

        return IOTableExchanges(
            Exchange(input=i.key, output=self.key, amount=v, type="biosphere")
            for i, v in zip(b_inputs, b_vals)
        )

    def production(self):
        lca, rev_act_dict, rev_bio_dict = self._construct_or_load_buffer()

        # look up technosphere inputs
        col = lca.dicts.activity[self.id]
        val = lca.technosphere_matrix[col, col]
        return IOTableExchanges(
            [Exchange(input=self.key, output=self.key, amount=val, type="production")]
        )

    def exchanges(self):
        return IOTableExchanges(
            itertools.chain(self.production(), self.technosphere(), self.biosphere())
        )
