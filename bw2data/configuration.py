import platform
from pathlib import Path
from typing import List, Optional

from deprecated import deprecated
from pydantic_settings import BaseSettings, SettingsConfigDict


class MatrixLabels(BaseSettings):
    lci_node_types: List[str] = [
        "process",
        "product",
        "processwithreferenceproduct",
        "multifunctional",
    ]
    other_node_types: List[str] = []
    process_node_types: List[str] = ["process", "processwithreferenceproduct"]
    product_node_types: List[str] = ["product"]

    process_node_default: str = "process"
    multifunctional_node_default: str = "multifunctional"
    chimaera_node_default: str = "processwithreferenceproduct"
    product_node_default: str = "product"
    biosphere_node_default: str = "emission"

    biosphere_edge_types: List[str] = ["biosphere"]
    technosphere_negative_edge_types: List[str] = [
        "technosphere",
        "generic consumption",
    ]
    technosphere_positive_edge_types: List[str] = [
        "production",
        "generic production",
        "substitution",
    ]
    # You should normally use `technosphere_positive_edge_types`, as it includes substitution
    substitution_edge_types: List[str] = ["substitution"]
    other_edge_types: List[str] = []

    production_edge_default: str = "production"
    consumption_edge_default: str = "technosphere"
    biosphere_edge_default: str = "biosphere"
    substitution_edge_default: str = "substitution"

    @property
    def implicit_production_allowed_node_types(self):
        return [self.chimaera_node_default]

    @property
    def lci_edge_types(self) -> List[str]:
        return sorted(
            set(
                self.biosphere_edge_types
                + self.technosphere_negative_edge_types
                + self.technosphere_positive_edge_types
                + self.substitution_edge_types
            )
        )

    @property
    def edge_types(self) -> List[str]:
        return sorted(set(self.lci_edge_types + self.other_edge_types))

    @property
    def node_types(self) -> List[str]:
        return sorted(set(self.lci_node_types + self.other_node_types))

    model_config = SettingsConfigDict(
        env_file="brightway-matrix-configuration.env",
        env_prefix="dont_pick_up_random_env_vars_a1b2c3d4e5",
    )

    def reload(self, fp: Path) -> None:
        """Load new `.env` file and overwrite settings"""
        self.model_config.update(env_file=fp)


class TypoSettings(BaseSettings):
    node_types: List[str] = [
        "economic",
        "emission",
        "inventory indicator",
        "multifunctional",
        "natural resource",
        "process",
        "processwithreferenceproduct",
        "product",
    ]
    edge_types: List[str] = [
        "biosphere",
        "generic consumption",
        "generic production",
        "production",
        "substitution",
        "technosphere",
    ]
    node_keys: List[str] = [
        "CAS number",
        "activity",
        "activity type",
        "authors",
        "categories",
        "classifications",
        "code",
        "comment",
        "created",
        "database",
        "exchanges",
        "filename",
        "flow",
        "id",
        "location",
        "modified",
        "name",
        "parameters",
        "production amount",
        "reference product",
        "synonyms",
        "tags",
        "type",
        "unit",
    ]
    edge_keys: List[str] = [
        "shape",
        "temporal_distribution",
        "activity",
        "amount",
        "classifications",
        "code",
        "comment",
        "flow",
        "input",
        "loc",
        "maximum",
        "minimum",
        "name",
        "output",
        "pedigree",
        "production volume",
        "properties",
        "scale without pedigree",
        "scale",
        "type",
        "uncertainty type",
        "uncertainty_type",
        "unit",
    ]

    model_config = SettingsConfigDict(
        env_file="brightway-typo-configuration.env",
        env_prefix="dont_pick_up_random_env_vars_a1b2c3d4e5",
    )


class Config(BaseSettings):
    version: int = 3
    backends: dict = {}
    cache: dict = {}
    metadata: list = []
    sqlite3_databases: list = []
    # When the LCI SQLite file releases free pages; see ``SubstitutableDatabase.vacuum``.
    # None (default): ``Database.delete()`` and ``Database.write()`` release them, with
    # ``PRAGMA incremental_vacuum`` in files with ``auto_vacuum = INCREMENTAL`` (new files,
    # and older ones after their next ``VACUUM``), otherwise with ``VACUUM`` when a
    # meaningful share of the file is free pages. A write only leaves free pages if the
    # database got smaller: its new rows use the pages of the rows it replaces.
    # True: always ``VACUUM``. False: never.
    vacuum: Optional[bool] = None
    # Upper limit of SQLite's page cache during ``Database.write()``, in KiB (default:
    # 256 MiB). Keeping the indexes up to date during a large write is much faster with a
    # large cache. SQLite only takes the memory as it caches pages, and gives it back after
    # the write. Each connection (process) has its own cache: lower it where memory is
    # short, or set it to 0 to keep SQLite's default (2 MiB).
    write_cache_size: int = 262_144
    # Passed to ``asteval.Interpreter`` as ``max_statement_length``; asteval caps it at 1e8
    max_formula_length: int = 50_000
    # Extra classes ``bw2data.codec.safe_pickle_loads`` may load from data stored by older
    # versions, as ``"module.name"`` strings (e.g. ``"mypackage.MyClass"``). Only add classes
    # from packages you trust: loading a pickle can run code.
    trusted_pickle_classes: List[str] = []
    _windows: bool = platform.system() == "Windows"

    model_config = SettingsConfigDict(
        env_file="brightway-configuration.env",
        extra="allow",
        env_prefix="dont_pick_up_random_env_vars_a1b2c3d4e5",
    )

    @property
    def biosphere(self):
        """Get name for ``biosphere`` database from user preferences.

        Default name is ``biosphere3``; change this by changing ``config.p["biosphere_database"]``.
        """
        return self.p.get("biosphere_database", "biosphere3")

    @property
    def global_location(self):
        """Get name for global location from user preferences.

        Default name is ``GLO``; change this by changing ``config.p["global_location"]``.
        """
        return self.p.get("global_location", "GLO")


labels = MatrixLabels()
typo_settings = TypoSettings()
config = Config()
