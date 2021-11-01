__all__ = [
    "dynamic_calculation_setups",
    "calculation_setups",
    "config",
    "convert_backend",
    "Database",
    "databases",
    "DataStore",
    "Edge",
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

from .configuration import config
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
from .project import projects
from .utils import set_data_dir
from .version import version as __version__

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

from .backends import Edge, Node, convert_backend, get_id
from .compat import Mapping, prepare_lca_inputs
from .data_store import DataStore, ProcessedDataStore
from .database import DatabaseChooser as Database
from .method import Method
from .search import IndexManager, Searcher
from .serialization import JsonWrapper
from .utils import get_activity, get_node
from .weighting_normalization import Normalization, Weighting

mapping = Mapping()

from .parameters import parameters
from .updates import Updates

Updates.check_status()
