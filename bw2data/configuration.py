import platform
from pathlib import Path
from typing import List, Union

from pydantic_settings import BaseSettings, SettingsConfigDict


class MatrixLabels(BaseSettings):
    process_node_default: str = "process"
    production_edge_default: str = "production"
    consumption_edge_default: str = "technosphere"
    biosphere_edge_default: str = "biosphere"

    process_node_types: List[Union[str, None]] = ["process", None]
    node_types: List[Union[str, None]] = ["process", "product", "multifunctional", None]

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
    substitution_edge_types: List[str] = ["substitution"]

    @property
    def edge_types(self):
        return sorted(
            set(
                self.biosphere_edge_types
                + self.technosphere_negative_edge_types
                + self.technosphere_positive_edge_types
                + self.substitution_edge_types
            )
        )

    model_config = SettingsConfigDict(env_file="brightway-matrix-configuration.env")

    def reload(self, fp: Path) -> None:
        """Load new `.env` file and overwrite settings"""
        self.model_config.update(env_file=fp)


class TypoSettings(BaseSettings):
    node_types: List[str] = [
        "economic",
        "emission",
        "inventory indicator",
        "natural resource",
        "process",
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

    model_config = SettingsConfigDict(env_file="brightway-typo-configuration.env")


class Config(BaseSettings):
    version: int = 3
    backends: dict = {}
    cache: dict = {}
    metadata: list = []
    sqlite3_databases: list = []
    _windows: bool = platform.system() == "Windows"

    model_config = SettingsConfigDict(
        env_file="brightway-configuration.env", extra="allow"
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
