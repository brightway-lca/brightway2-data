import warnings

from . import (
    Database,
    Method,
    Normalization,
    Weighting,
    methods,
    normalizations,
    projects,
    weightings,
)
from .backends.schema import ActivityDataset as AD
from .backends.schema import get_id
from .errors import Brightway2Project


class Mapping:
    """A dictionary that maps object ids, like ``("Ecoinvent 2.2", 42)``, to integers.

    Used only for backwards compatibility; preferred method is now to look up the ids of activities directly in the SQlite database."""

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


class _Databases:
    def clean(self):
        warnings.warn(
            "Use `Database.clean_all()` instead of `databases.clean()`",
            DeprecationWarning,
        )
        Database.clean_all()

    def set_dirty(self, name):
        warnings.warn(
            "Use `Database.set_dirty(name)` instead of `databases.set_dirty(name)`",
            DeprecationWarning,
        )
        Database.set_dirty(name)

    def __getitem__(self, name):
        warnings.warn(
            "Use `Database` attributes directly instead of `databases[name]`",
            DeprecationWarning,
        )
        return Database.get(Database.name == name).metadata

    def __contains__(self, name):
        warnings.warn(
            "Use `Database.exists(name)` instead of `name in databases`",
            DeprecationWarning,
        )
        return Database.exists(name)

    def __len__(self):
        warnings.warn(
            "Use `Database.select().count()` instead of `len(databases)`",
            DeprecationWarning,
        )
        return len(Database)

    def __delitem__(self, name):
        warnings.warn(
            "Use `Database(name).delete()` instead of `del databases[name]`",
            DeprecationWarning,
        )
        Database.get(Database.name == name).delete_instance()

    def __iter__(self):
        return (obj.name for obj in Database.select())

    def flush(self):
        warnings.warn(
            "`databases.flush()` doesn't do anything. Modify `Database` attributes and call `.save()` instead.",
            DeprecationWarning,
        )

    def __setitem__(self, *args, **kwargs):
        warnings.warn(
            "`databases[foo] = something` doesn't do anything. Modify `Database` attributes and call `.save()` instead.",
            DeprecationWarning,
        )


databases = _Databases()


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
        demand_database_names = [
            db_label for dct in demands for db_label, _ in unpack(dct)
        ]
    elif demand:
        demand_database_names = [db_label for db_label, _ in unpack(demand)]
    else:
        demand_database_names = []

    if demand_database_names:
        database_names = set.union(
            *[
                Database(db_label).find_graph_dependents()
                for db_label in demand_database_names
            ]
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
    dbs = set.union(
        *[Database(key[0]).find_graph_dependents() for key in functional_unit]
    )
    return [Database(obj).filepath_processed() for obj in dbs]
