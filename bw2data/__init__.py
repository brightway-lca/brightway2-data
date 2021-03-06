# -*- coding: utf-8 -*-
__all__ = [
    "dynamic_calculation_setups",
    "calculation_setups",
    "config",
    "convert_backend",
    "Database",
    "databases",
    "DataStore",
    "get_activity",
    "geomapping",
    "IndexManager",
    "JsonWrapper",
    "mapping",
    "Method",
    "methods",
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

from .version import version as __version__

from .configuration import config
from .project import projects
from .utils import set_data_dir
from .meta import (
    dynamic_calculation_setups,
    calculation_setups,
    databases,
    geomapping,
    mapping,
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
        mapping,
        methods,
        normalizations,
        preferences,
        weightings,
    ]
)

# Backwards compatibility - preferable to access ``preferences`` directly
config.p = preferences

from .serialization import JsonWrapper
from .database import DatabaseChooser as Database, get_activity
from .data_store import DataStore, ProcessedDataStore
from .method import Method
from .search import Searcher, IndexManager
from .weighting_normalization import Weighting, Normalization
from .backends import convert_backend
from .compat import prepare_lca_inputs

# Don't confuse nose tests
from .updates import Updates
from .parameters import parameters

projects.set_current("default")

Updates.check_status()
