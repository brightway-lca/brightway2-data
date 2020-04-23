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


def prepare_lca_inputs(
    demand=None, method=None, weighting=None, normalization=None, remapping=True
):
    """Prepare LCA input arguments in Brightway 3 style."""
    databases.clean()
    data_objs = []
    if demand:
        data_objs.extend([
            Database(obj).filepath_processed()
            for obj in set.union(
                *[
                    Database(db_label).find_graph_dependents()
                    for db_label, _ in demand
                ]
            )
        ])
    if method:
        assert method in methods
        data_objs.append(Method(method).filepath_processed())
    if weighting:
        assert weighting in weightings
        data_objs.append(Weighting(weighting).filepath_processed())
    if normalization:
        assert normalization in normalizations
        data_objs.append(Normalization(normalization).filepath_processed())

    if remapping:
        reversed_mapping = {v: k for k, v in mapping.items()}
        remapping_dicts = {
            "activity": reversed_mapping,
            "product": reversed_mapping,
            "biosphere": reversed_mapping,
        }
    else:
        remapping = {}

    indexed_demand = (
        None if demand is None else {mapping[k]: v for k, v in demand.items()}
    )

    return indexed_demand, data_objs, remapping_dicts


def get_database_filepath(functional_unit):
    """Get filepaths for all databases in supply chain of `functional_unit`"""
    dbs = set.union(
        *[Database(key[0]).find_graph_dependents() for key in functional_unit]
    )
    return [Database(obj).filepath_processed() for obj in dbs]
