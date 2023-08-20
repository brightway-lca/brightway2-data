__all__ = [
    "dynamic_calculation_setups",
    "calculation_setups",
    "config",
    "convert_backend",
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

import importlib.metadata
from typing import Union


def get_version_tuple() -> tuple:
    def as_integer(x: str) -> Union[int, str]:
        try:
            return int(x)
        except ValueError:
            return x

    return tuple(
        as_integer(v) for v in importlib.metadata.version("bw2data").strip().split(".")
    )


__version__ = get_version_tuple()

from .configuration import config
from .project import projects
from .utils import set_data_dir
from .meta import (
    calculation_setups,
    databases,
    dynamic_calculation_setups,
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
        databases,
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
from .database import DatabaseChooser as Database
from .utils import get_activity, get_node
from .data_store import DataStore, ProcessedDataStore
from .method import Method
from .search import Searcher, IndexManager
from .weighting_normalization import Weighting, Normalization
from .backends import convert_backend, get_id, Node, Edge
from .compat import prepare_lca_inputs, Mapping
from .backends.wurst_extraction import extract_brightway_databases

mapping = Mapping()

from .parameters import parameters
from .updates import Updates

Updates.check_status()
