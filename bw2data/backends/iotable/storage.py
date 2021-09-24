from ...compat import prepare_lca_inputs
from ... import Database
from bw2calc import LCA
import pandas as pd


class Storage(object):

    buffer = dict()

    @classmethod
    def construct_or_load_metadata(cls, databases):

        # load existing data
        if "df_meta" in cls.buffer:
            df_meta = cls.buffer["df_meta"]
        else:
            df_meta = pd.DataFrame()

        # construct dataframes containing activity and biosphere metadata
        for db in databases:
            # check if dataframe for this database has been constructed previously
            # load if yes
            if "database" in df_meta and db in df_meta["database"]:
                continue
            # construct otherwise
            else:
                df_meta = pd.concat([df_meta, pd.DataFrame(Database(db).load()).T])

        # store extended dataframe
        df_meta.index.names = ['database', 'code']
        cls.buffer["df_meta"] = df_meta

        return df_meta

    @classmethod
    def construct_or_load_matrices(cls, db_name):
        # load or construct helper LCA object containing technosphere and biosphere data

        # check if matrices have been constructed previously, load if yes
        dependents = tuple(Database(db_name).find_graph_dependents())
        if dependents in cls.buffer:
            return cls.buffer[dependents]

        # otherwise construct new lca object and save in storage
        else:
            demand, data_objs, remapping_dicts = prepare_lca_inputs(
                {Database(db_name).random().key: 1}
            )
            lca = LCA(
                demand=demand, data_objs=data_objs, remapping_dicts=remapping_dicts
            )
            lca.load_lci_data()
            lca.remap_inventory_dicts()
            rev_act_dict = pd.Series(lca.dicts.activity.reversed)
            rev_bio_dict = pd.Series(lca.dicts.biosphere.reversed)
            cls.buffer[dependents] = (lca, rev_act_dict, rev_bio_dict)

        return lca, rev_act_dict, rev_bio_dict

    @classmethod
    def flush(cls):
        del cls.buffer
        cls.buffer = {}
