from .. import SQLiteBackend
from ... import geomapping, config, databases
from bw_processing import create_datapackage, clean_datapackage_name
from fs.zipfs import ZipFS
import datetime


class IOTableBackend(SQLiteBackend):
    """IO tables have too much data to store each value in a database; instead, we only store the processed data in NumPy arrays.

    Activities will not seem to have any activities."""

    backend = "iotable"

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

        dp = create_datapackage(
            fs=ZipFS(str(self.filepath_processed()), write=True),
            name=clean_datapackage_name(self.name),
            sum_intra_duplicates=True,
            sum_inter_duplicates=False,
        )

        dp.add_persistent_vector_from_iterator(
            dict_iterator=(
                {
                    "row": obj.id,
                    "col": geomapping[obj["location"] or config.global_location],
                    "amount": 1,
                }
                for obj in self
            ),
            matrix="inv_geomapping_matrix",
            name=clean_datapackage_name(self.name + " inventory geomapping matrix"),
            nrows=len(self),
        )
        print("Adding technosphere matrix")
        dp.add_persistent_vector_from_iterator(
            matrix="technosphere_matrix",
            name=clean_datapackage_name(self.name + " technosphere matrix"),
            dict_iterator=technosphere,
        )

        print("Adding biosphere matrix")
        dp.add_persistent_vector_from_iterator(
            matrix="biosphere_matrix",
            name=clean_datapackage_name(self.name + " biosphere matrix"),
            dict_iterator=biosphere,
        )
        dp.finalize_serialization()

        databases[self.name]["depends"] = sorted(set(dependents).difference({self.name}))
        databases[self.name]["processed"] = datetime.datetime.now().isoformat()
        databases.flush()

    def process(self):
        """No-op; no intermediate data to process"""
        return
