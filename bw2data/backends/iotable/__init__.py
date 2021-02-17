from .. import SQLiteBackend, get_id
from ... import geomapping, config, databases
from ...errors import UnknownObject
from bw_processing import create_datapackage, clean_datapackage_name
from fs.zipfs import ZipFS
import datetime
import itertools


class IOTableBackend(SQLiteBackend):
    """IO tables have too much data to store each value in a database; instead, we only store the processed data in NumPy arrays.

    Activities will not seem to have any activities."""

    backend = "iotable"

    def write(self, products, prod_exchanges, tech_exchanges, bio_exchanges, **kwargs):
        """

        Write IO data to disk in two different formats.

        Product data is stored in SQLite as normal activities.
        Exchange data is written directly to NumPy structured arrays.

        ``products`` is a dictionary of product datasets in the normal format.

        ``tech_exchanges`` and ``bio_exchanges`` are lists  of exchanges with the format ``(input code, output code, type, value)``.

        """
        super(IOTableBackend, self).write(products, process=False)
        print("Starting IO table write")

        def as_iterator(exchanges, dependents, flip=False, step=1000000):
            for index, row in enumerate(exchanges):
                if index and not index % step:
                    print("On exchange number {}".format(index))
                try:
                    dependents.addd(row[0][0])
                    yield {
                        "row": get_id(row[0]),
                        "col": get_id(row[1]),
                        "flip": flip,
                        "amount": row[2],
                        "uncertainty_type": 0,
                    }
                except UnknownObject:
                    raise UnknownObject(
                        (
                            "Exchange between {} and {} is invalid "
                            "- one of these objects is unknown (i.e. doesn't exist "
                            "as a process dataset)"
                        ).format(row[0], row[1])
                    )

        dp = create_datapackage(
            fs=ZipFS(str(self.filepath_processed()), write=True),
            name=clean_datapackage_name(self.name),
            sum_intra_duplicates=True,
            sum_inter_duplicates=False,
        )
        dp.add_persistent_vector_from_iterator(
            dict_iterator=(
                {
                    "row": get_id(key),
                    "col": geomapping[value["location"] or config.global_location],
                    "amount": 1,
                }
                for index, (key, value) in enumerate(sorted(products.items()))
            ),
            matrix="inv_geomapping_matrix",
            name=clean_datapackage_name(self.name + " inventory geomapping matrix"),
            nrows=len(products),
        )

        dependents = set()

        print("Creating arrays - this will take a while...")

        dp.add_persistent_vector_from_iterator(
            matrix="technosphere_matrix",
            name=clean_datapackage_name(self.name + " technosphere matrix"),
            dict_iterator=itertools.chain(
                as_iterator(tech_exchanges, dependents, flip=True),
                as_iterator(prod_exchanges, dependents, flip=False),
            ),
        )

        dp.add_persistent_vector_from_iterator(
            matrix="biosphere_matrix",
            name=clean_datapackage_name(self.name + " biosphere matrix"),
            dict_iterator=as_iterator(bio_exchanges, dependents, flip=True),
        )

        databases[self.name]["depends"] = sorted(dependents.difference({self.name}))
        databases[self.name]["processed"] = datetime.datetime.now().isoformat()
        databases.flush()

    def process(self):
        """No-op; no intermediate data to process"""
        return
