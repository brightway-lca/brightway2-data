from . import (
    Database,
    databases,
    normalizations,
    Normalization,
    weightings,
    Weighting,
    methods,
    Method,
)
from .backends.schema import ActivityDataset as AD, get_id
from fs.zipfs import ZipFS


class Mapping:
    """A dictionary that maps object ids, like ``("Ecoinvent 2.2", 42)``, to integers.

    Used only for backwards compatibility; preferred methodd is now to look up the ids of activities directly in the SQlite database."""

    def add(self, keys):
        raise DeprecationWarning(
            "This method is no longer necessary, and does nothing."
        )
        return

    def __getitem__(self, key):
        return get_id(key)

    def delete(self, keys):
        raise DeprecationWarning(
            "This method is no longer necessary, and does nothing."
        )
        return

    def __str__(self):
        return "Obsolete mapping dictionary."

    def __len__(self):
        return AD.select().count()


def unpack(dct):
    for obj in dct:
        if hasattr(obj, "key"):
            yield obj.key
        else:
            yield obj


def translate_key(key):
    if isinstance(key, int):
        return key
    else:
        return AD.get(AD.database == key[0], AD.code == key[1]).id


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
        database_names = set.union(
            *[
                Database(db_label).find_graph_dependents()
                for dct in demands
                for db_label, _ in unpack(dct)
            ]
        )
    elif demand:
        database_names = set.union(
            *[
                Database(db_label).find_graph_dependents()
                for db_label, _ in unpack(demand)
            ]
        )
    else:
        raise ValueError("Need some form of demand for LCA calculation")

    if demands:
        data_objs.extend(
            [ZipFS(Database(obj).filepath_processed()) for obj in database_names]
        )
    elif demand:
        data_objs.extend(
            [ZipFS(Database(obj).filepath_processed()) for obj in database_names]
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
        reversed_mapping = {
            i: (d, c)
            for d, c, i in AD.select(AD.database, AD.code, AD.id)
            .where(AD.database << database_names)
            .tuples()
        }
        remapping_dicts = {
            "activity": reversed_mapping,
            "product": reversed_mapping,
            "biosphere": reversed_mapping,
        }
    else:
        remapping = {}

    if demands:
        indexed_demand = [{get_id(k): v for k, v in dct.items()} for dct in demands]
    elif demand:
        indexed_demand = {get_id(k): v for k, v in demand.items()}
    else:
        indexed_demand = None

    return indexed_demand, data_objs, remapping_dicts


def get_database_filepath(functional_unit):
    """Get filepaths for all databases in supply chain of `functional_unit`"""
    dbs = set.union(
        *[Database(key[0]).find_graph_dependents() for key in functional_unit]
    )
    return [Database(obj).filepath_processed() for obj in dbs]
