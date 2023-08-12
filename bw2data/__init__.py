__all__ = [
    "dynamic_calculation_setups",
    "calculation_setups",
    "config",
    "Database",
    "databases",
    "DataStore",
    "Edge",
    "extract_brightway_databases",
    "get_activity",
    "get_node",
    "get_id",
    "geomapping",
    "IndexManager",
    "JsonWrapper",
    "mapping",
    "Method",
    "methods",
    "Node",
    "Normalization",
    "normalizations",
    "parameters",
    "preferences",
    "prepare_lca_inputs",
    "ProcessedDataStore",
    "projects",
    "Searcher",
    "set_data_dir",
    "Weighting",
    "weightings",
]

import importlib
from typing import Union


def get_version_tuple() -> tuple:
    """Returns version as (major, minor, micro)."""

    def as_integer(version: str) -> Union[int, str]:
        """Tries parsing version else returns as is."""
        try:
            return int(version)
        except ValueError:  # pragma: no cover
            return version  # pragma: no cover

    return tuple(
        as_integer(v)
        for v in importlib.metadata.version("bw2data").strip().split(".")
    )


__version__ = get_version_tuple()


from .configuration import config
from .project import projects
from .utils import set_data_dir
from .meta import (
    dynamic_calculation_setups,
    calculation_setups,
    geomapping,
    methods,
    normalizations,
    preferences,
    weightings,
)

# Add metadata class instances to global list of serialized metadata
config.metadata.extend(
    [
        dynamic_calculation_setups,
        calculation_setups,
        geomapping,
        methods,
        normalizations,
        preferences,
        weightings,
    ]
)

# Backwards compatibility - preferable to access ``preferences`` directly
config.p = preferences

from .serialization import JsonWrapper
from .backends import Database
from .utils import get_activity, get_node
from .data_store import DataStore, ProcessedDataStore
from .method import Method
from .search import Searcher, IndexManager
from .weighting_normalization import Weighting, Normalization
from .backends import get_id, Node, Edge
from .compat import prepare_lca_inputs, Mapping, databases
from .backends.wurst_extraction import extract_brightway_databases

mapping = Mapping()

from .updates import Updates
from .parameters import parameters

Updates.check_status()
