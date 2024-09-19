from typing import Dict, List, Union

from bw_processing.datapackage import DatapackageBase

from bw2data import (
    Database,
    Method,
    Normalization,
    Weighting,
    databases,
    get_node,
    methods,
    normalizations,
    projects,
    weightings,
)
from bw2data.backends.schema import ActivityDataset as AD
from bw2data.backends.schema import get_id
from bw2data.errors import Brightway2Project, UnknownObject


class Mapping:
    """A dictionary that maps object ids, like ``("Ecoinvent 2.2", 42)``, to integers.

    Used only for backwards compatibility; preferred method is now to look up the ids of activities directly in the SQlite database.
    """

    def add(self, keys):
        raise DeprecationWarning("This method is no longer necessary, and does nothing.")
        return

    def __getitem__(self, key):
        return get_id(key)

    def delete(self, keys):
        raise DeprecationWarning("This method is no longer necessary, and does nothing.")
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
    demand_database_last=True,
):
    """Prepare LCA input arguments in Brightway 2.5 style."""
    if not projects.dataset.data.get("25"):
        raise Brightway2Project(
            "Please use `projects.migrate_project_25` before calculating using Brightway 2.5"
        )

    databases.clean()
    data_objs = []
    remapping_dicts = None

    if demands:
        demand_database_names = [db_label for dct in demands for db_label, _ in unpack(dct)]
    elif demand:
        demand_database_names = [db_label for db_label, _ in unpack(demand)]
    else:
        demand_database_names = []

    if demand_database_names:
        database_names = set.union(
            *[Database(db_label).find_graph_dependents() for db_label in demand_database_names]
        )

        if demand_database_last:
            database_names = [
                x for x in database_names if x not in demand_database_names
            ] + demand_database_names

        data_objs.extend([Database(obj).datapackage() for obj in database_names])

        if remapping:
            # This is technically wrong - we could have more complicated queries
            # to determine what is truly a product, activity, etc.
            # However, for the default database schema, we know that each node
            # has a unique ID, so this won't produce incorrect responses,
            # just too many values. As the dictionary only exists once, this is
            # not really a problem.
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

    if method:
        assert method in methods
        data_objs.append(Method(method).datapackage())
    if weighting:
        assert weighting in weightings
        data_objs.append(Weighting(weighting).datapackage())
    if normalization:
        assert normalization in normalizations
        data_objs.append(Normalization(normalization).datapackage())

    if demands:
        indexed_demand = [{get_id(k): v for k, v in dct.items()} for dct in demands]
    elif demand:
        indexed_demand = {get_id(k): v for k, v in demand.items()}
    else:
        indexed_demand = None

    return indexed_demand, data_objs, remapping_dicts


def get_database_filepath(functional_unit):
    """Get filepaths for all databases in supply chain of `functional_unit`"""
    dbs = set.union(*[Database(key[0]).find_graph_dependents() for key in functional_unit])
    return [Database(obj).filepath_processed() for obj in dbs]


def get_multilca_data_objs(
    functional_units=Dict[str, dict], method_config=Dict[str, Union[list, dict]]
) -> List[DatapackageBase]:
    """Get all the datapackages needed for a complete MultiLCA calculation."""
    input_database_names = set()

    for v_dict in functional_units.values():
        for obj in v_dict:
            if not isinstance(obj, int):
                raise ValueError(
                    f"Functional unit inputs must be integers; got {obj} (type {type(obj)})"
                )
            try:
                input_database_names.add(get_node(id=obj)["database"])
            except UnknownObject:
                raise UnknownObject(f"Functional unit id {obj} is not in this project")

    complete_database_names = set.union(
        *[Database(db_label).find_graph_dependents() for db_label in input_database_names]
    )
    data_objs = [Database(obj).datapackage() for obj in complete_database_names]

    for ic in set(method_config.get("impact_categories", [])):
        if ic not in methods:
            raise ValueError(f"Impact category (`Method`) {ic} not in this project")
        data_objs.append(Method(ic).datapackage())

    for n in method_config.get("normalizations", []):
        if n not in normalizations:
            raise ValueError(f"Normalization {n} not in this project")
        data_objs.append(Normalization(n).datapackage())

    for w in method_config.get("weightings", []):
        if w not in weightings:
            raise ValueError(f"Weighting {w} not in this project")
        data_objs.append(Weighting(w).datapackage())

    return data_objs
