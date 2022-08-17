from tqdm import tqdm
import functools
import datetime
import itertools

from bw_processing import clean_datapackage_name, create_datapackage
from fs.zipfs import ZipFS

import pandas as pd
from typing import Callable, Optional, List

from ... import config, databases, geomapping
from .. import SQLiteBackend
from .proxies import IOTableActivity, IOTableExchanges


class IOTableBackend(SQLiteBackend):
    """IO tables have too much data to store each value in a database; instead, we only store the processed data in NumPy arrays.

    Activities will not seem to have any exchanges."""

    backend = "iotable"
    node_class = IOTableActivity

    def write(self, data):
        super().write(data, process=False)

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
            fs=ZipFS(str(self.filepath_processed()), write=True),
            name=clean_datapackage_name(self.name),
            sum_intra_duplicates=True,
            sum_inter_duplicates=False,
        )

        # add geomapping
        dp.add_persistent_vector_from_iterator(
            dict_iterator=(
                {
                    "row": obj.id,
                    "col": geomapping[
                        obj.get("location", None) or config.global_location
                    ],
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
            raise Exception(
                f"Error: Unsupported technosphere type: {type(technosphere)}"
            )

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

        databases[self.name]["depends"] = sorted(
            set(dependents).difference({self.name})
        )
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
        from ... import get_activity

        @functools.lru_cache(10000)
        def cached_lookup(code):
            return get_activity(id=code)

        print("Retrieving metadata")
        activities = {o.id: o for o in self}
        exchanges = IOTableExchanges(datapackage=self.datapackage())
        exchanges_iterator = [
            zip(exchanges._raw_technosphere_iterator(negative=False), itertools.repeat("production")),
            zip(exchanges._raw_technosphere_iterator(negative=True), itertools.repeat("technosphere")),
            zip(exchanges._raw_biosphere_iterator(), itertools.repeat("biosphere")),
        ]

        result = []

        print("Iterating over exchanges")
        pbar = tqdm(total=len(exchanges))
        for (row, col, value), type_label in itertools.chain(*exchanges_iterator):
            target = activities[col]
            try:
                source = activities[row]
            except KeyError:
                source = cached_lookup(row)

            row = {
                "target_id": target["id"],
                "target_database": target["database"],
                "target_code": target["code"],
                "target_name": target.get("name"),
                "target_reference_product": target.get("reference product"),
                "target_location": target.get("location"),
                "target_unit": target.get("unit"),
                "target_type": target.get("type", "process"),
                "source_id": source["id"],
                "source_database": source["database"],
                "source_code": source["code"],
                "source_name": source.get("name"),
                "source_product": source.get("product"),
                "source_location": source.get("location"),
                "source_unit": source.get("unit"),
                "source_categories": "::".join(source["categories"]) if source.get("categories") else None,
                "edge_amount": value,
                "edge_type": type_label,
            }
            result.append(row)
            pbar.update(1)

        pbar.close()

        print("Creating DataFrame")
        df = pd.DataFrame(result)

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
