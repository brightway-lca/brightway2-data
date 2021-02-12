from . import (
    mapping,
    Database,
    databases,
    normalizations,
    Normalization,
    weightings,
    Weighting,
    methods,
    Method,
)
from fs.zipfs import ZipFS


def unpack(dct):
    for obj in dct:
        if hasattr(obj, "key"):
            yield obj.key
        else:
            yield obj


def prepare_lca_inputs(
    demand=None,
    method=None,
    weighting=None,
    normalization=None,
    demands=None,
    remapping=True,
):
    """Prepare LCA input arguments in Brightway 3 style."""
    databases.clean()
    data_objs = []
    if demands:
        data_objs.extend(
            [
                ZipFS(Database(obj).filepath_processed())
                for obj in set.union(
                    *[
                        Database(db_label).find_graph_dependents()
                        for dct in demands
                        for db_label, _ in unpack(dct)
                    ]
                )
            ]
        )
    elif demand:
        data_objs.extend(
            [
                ZipFS(Database(obj).filepath_processed())
                for obj in set.union(
                    *[
                        Database(db_label).find_graph_dependents()
                        for db_label, _ in unpack(demand)
                    ]
                )
            ]
        )
    if method:
        assert method in methods
        data_objs.append(ZipFS(Method(method).filepath_processed()))
    if weighting:
        assert weighting in weightings
        data_objs.append(ZipFS(Weighting(weighting).filepath_processed()))
    if normalization:
        assert normalization in normalizations
        data_objs.append(ZipFS(Normalization(normalization).filepath_processed()))

    if remapping:
        reversed_mapping = {v: k for k, v in mapping.items()}
        remapping_dicts = {
            "activity": reversed_mapping,
            "product": reversed_mapping,
            "biosphere": reversed_mapping,
        }
    else:
        remapping = {}

    if demands:
        indexed_demand = [{mapping[k]: v for k, v in dct.items()} for dct in demands]
    elif demand:
        indexed_demand = {mapping[k]: v for k, v in demand.items()}
    else:
        indexed_demand = None

    return indexed_demand, data_objs, remapping_dicts


def get_database_filepath(functional_unit):
    """Get filepaths for all databases in supply chain of `functional_unit`"""
    dbs = set.union(
        *[Database(key[0]).find_graph_dependents() for key in functional_unit]
    )
    return [Database(obj).filepath_processed() for obj in dbs]
