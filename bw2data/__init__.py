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
    "get_multilca_data_objs",
    "get_node",
    "get_id",
    "geomapping",
    "IndexManager",
    "JsonWrapper",
    "labels",
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

__version__ = (4, 0, "dev55")

from bw2data.configuration import config, labels
from bw2data.project import projects
from bw2data.utils import set_data_dir
from bw2data.meta import (
    dynamic_calculation_setups,
    calculation_setups,
    databases,
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

from bw2data.serialization import JsonWrapper
from bw2data.database import DatabaseChooser as Database
from bw2data.utils import get_activity, get_node
from bw2data.data_store import DataStore, ProcessedDataStore
from bw2data.method import Method
from bw2data.search import Searcher, IndexManager
from bw2data.weighting_normalization import Weighting, Normalization
from bw2data.backends import convert_backend, get_id, Node, Edge
from bw2data.compat import prepare_lca_inputs, Mapping, get_multilca_data_objs
from bw2data.backends.wurst_extraction import extract_brightway_databases

mapping = Mapping()

from bw2data.updates import Updates
from bw2data.parameters import parameters

Updates.check_status()


try:
    # Will register itself as a database backend provider
    import multifunctional
except ImportError:
    pass
