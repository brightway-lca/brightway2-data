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
        # if technosphere is a dictionary pass it's keys & values
        if isinstance(technosphere, dict):
            dp.add_persistent_vector(
                matrix="technosphere_matrix",
                name=clean_datapackage_name(self.name + " technosphere matrix"),
                **technosphere,
            )
        # if it is an iterable, convert to right format
        elif hasattr(technosphere, '__iter__'):
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
        elif hasattr(biosphere, '__iter__'):
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
