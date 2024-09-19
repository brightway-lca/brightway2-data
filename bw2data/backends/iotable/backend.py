import datetime
import functools

import numpy as np
import pandas as pd
from bw_processing import clean_datapackage_name, create_datapackage
from fsspec.implementations.zip import ZipFileSystem

from bw2data import config, databases, geomapping
from bw2data.backends import SQLiteBackend
from bw2data.backends.iotable.proxies import IOTableActivity, IOTableExchanges
from bw2data.configuration import labels


class IOTableBackend(SQLiteBackend):
    """IO tables have too much data to store each value in a database; instead, we only store the processed data in NumPy arrays.

    Activities will not seem to have any exchanges."""

    backend = "iotable"
    node_class = IOTableActivity

    def write(self, data, process=False, searchable=True, check_typos=True):
        super().write(data, process=False, searchable=searchable, check_typos=check_typos)

    def write_exchanges(self, technosphere, biosphere, dependents):
        """

        Write IO data directly to processed arrays.

        Product data is stored in SQLite as normal activities.
        Exchange data is written directly to NumPy structured arrays.

        Technosphere and biosphere data has format ``(row id, col id, value, flip)``.

        """
        print("Starting IO table write")

        # create empty datapackage
        dp = create_datapackage(
            fs=ZipFileSystem(self.filepath_processed(), mode="w"),
            name=clean_datapackage_name(self.name),
            sum_intra_duplicates=True,
            sum_inter_duplicates=False,
        )

        # add geomapping
        dp.add_persistent_vector_from_iterator(
            dict_iterator=(
                {
                    "row": obj.id,
                    "col": geomapping[obj.get("location", None) or config.global_location],
                    "amount": 1,
                }
                for obj in self
            ),
            matrix="inv_geomapping_matrix",
            name=clean_datapackage_name(self.name + " inventory geomapping matrix"),
            nrows=len(self),
        )

        print("Adding technosphere matrix")
        # if technosphere is a dictionary pass it's keys & values
        if isinstance(technosphere, dict):
            dp.add_persistent_vector(
                matrix="technosphere_matrix",
                name=clean_datapackage_name(self.name + " technosphere matrix"),
                **technosphere,
            )
        # if it is an iterable, convert to right format
        elif hasattr(technosphere, "__iter__"):
            dp.add_persistent_vector_from_iterator(
                matrix="technosphere_matrix",
                name=clean_datapackage_name(self.name + " technosphere matrix"),
                dict_iterator=technosphere,
            )
        else:
            raise Exception(f"Error: Unsupported technosphere type: {type(technosphere)}")

        print("Adding biosphere matrix")
        # if biosphere is a dictionary pass it's keys & values
        if isinstance(biosphere, dict):
            dp.add_persistent_vector(
                matrix="biosphere_matrix",
                name=clean_datapackage_name(self.name + " biosphere matrix"),
                **biosphere,
            )
        # if it is an iterable, convert to right format
        elif hasattr(biosphere, "__iter__"):
            dp.add_persistent_vector_from_iterator(
                matrix="biosphere_matrix",
                name=clean_datapackage_name(self.name + " biosphere matrix"),
                dict_iterator=biosphere,
            )
        else:
            raise Exception(f"Error: Unsupported biosphere type: {type(technosphere)}")

        # finalize
        print("Finalizing serialization")
        dp.finalize_serialization()

        databases[self.name]["depends"] = sorted(set(dependents).difference({self.name}))
        databases[self.name]["processed"] = datetime.datetime.now().isoformat()
        databases.flush()

    def process(self):
        """No-op; no intermediate data to process"""
        return

    def edges_to_dataframe(self) -> pd.DataFrame:
        """Return a pandas DataFrame with all database exchanges. DataFrame columns are:

            target_id: int,
            target_database: str,
            target_code: str,
            target_name: Optional[str],
            target_reference_product: Optional[str],
            target_location: Optional[str],
            target_unit: Optional[str],
            target_type: Optional[str]
            source_id: int,
            source_database: str,
            source_code: str,
            source_name: Optional[str],
            source_product: Optional[str],  # Note different label
            source_location: Optional[str],
            source_unit: Optional[str],
            source_categories: Optional[str]  # Tuple concatenated with "::" as in `bw2io`
            edge_amount: float,
            edge_type: str,

        Target is the node consuming the edge, source is the node or flow being consumed. The terms target and source were chosen because they also work well for biosphere edges.

        As IO Tables are normally quite large, the DataFrame building will operate directly on Numpy arrays, and therefore special formatters are not supported in this function.

        Returns a pandas ``DataFrame``.

        """
        from bw2data import get_node

        @functools.lru_cache(10000)
        def cached_lookup(id_):
            return get_node(id=id_)

        print("Retrieving metadata")
        activities = {o.id: o for o in self}

        def get(id_):
            try:
                return activities[id_]
            except KeyError:
                return cached_lookup(id_)

        def metadata_dataframe(ids, prefix="target_"):
            def dict_for_obj(obj, prefix):
                dct = {
                    f"{prefix}id": obj["id"],
                    f"{prefix}database": obj["database"],
                    f"{prefix}code": obj["code"],
                    f"{prefix}name": obj.get("name"),
                    f"{prefix}location": obj.get("location"),
                    f"{prefix}unit": obj.get("unit"),
                }
                if prefix == "target_":
                    dct["target_type"] = obj.get("type", labels.process_node_default)
                    dct["target_reference_product"] = obj.get("reference product")
                else:
                    dct["source_categories"] = (
                        "::".join(obj["categories"]) if obj.get("categories") else None
                    )
                    dct["source_product"] = obj.get("product")
                return dct

            return pd.DataFrame([dict_for_obj(get(id_), prefix) for id_ in np.unique(ids)])

        def get_edge_types(exchanges):
            arrays = []
            for resource in exchanges.resources:
                if resource["data"]["matrix"] == "biosphere_matrix":
                    arrays.append(
                        np.array([labels.biosphere_edge_default] * len(resource["data"]["array"]))
                    )
                else:
                    arr = np.array(
                        [labels.consumption_edge_default] * len(resource["data"]["array"])
                    )
                    arr[resource["flip"]["positive"]] = labels.production_edge_default
                    arrays.append(arr)

            return np.hstack(arrays)

        print("Loading datapackage")
        exchanges = IOTableExchanges(datapackage=self.datapackage())

        target_ids = np.hstack(
            [resource["indices"]["array"]["col"] for resource in exchanges.resources]
        )
        source_ids = np.hstack(
            [resource["indices"]["array"]["row"] for resource in exchanges.resources]
        )
        edge_amounts = np.hstack([resource["data"]["array"] for resource in exchanges.resources])
        edge_types = get_edge_types(exchanges)

        print("Creating metadata dataframes")
        target_metadata = metadata_dataframe(target_ids)
        source_metadata = metadata_dataframe(source_ids, "source_")

        print("Building merged dataframe")
        df = pd.DataFrame(
            {
                "target_id": target_ids,
                "source_id": source_ids,
                "edge_amount": edge_amounts,
                "edge_type": edge_types,
            }
        )
        df = df.merge(target_metadata, on="target_id")
        df = df.merge(source_metadata, on="source_id")

        categorical_columns = [
            "target_database",
            "target_name",
            "target_reference_product",
            "target_location",
            "target_unit",
            "target_type",
            "source_database",
            "source_code",
            "source_name",
            "source_product",
            "source_location",
            "source_unit",
            "source_categories",
            "edge_type",
        ]
        print("Compressing DataFrame")
        for column in categorical_columns:
            if column in df.columns:
                df[column] = df[column].astype("category")

        return df
