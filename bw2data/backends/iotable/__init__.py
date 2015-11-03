# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ... import mapping, geomapping, config, databases
from ..peewee import sqlite3_lci_db, SQLiteBackend
from ..peewee.schema import ActivityDataset, ExchangeDataset
from ..peewee.utils import dict_as_activitydataset
from ...utils import MAX_INT_32, TYPE_DICTIONARY
from ...errors import UnknownObject
import datetime
import itertools
import numpy as np
try:
    import cPickle as pickle
except ImportError:
    import pickle


class IOTableBackend(SQLiteBackend):
    """IO tables have too much data to store each value in a database; instead, we only store the processed data in NumPy arrays.

    Activities will not seem to have any activities."""
    backend = "iotable"

    def write(self, products, exchanges, **kwargs):
        """

        Write IO data to disk in two different formats.

        Product data is stored in SQLite as normal activities.
        Exchange data is written directly to NumPy structured arrays.

        ``products`` is a dictionary of product datasets in the normal format.

        ``exchanges`` is a list of exchanges with the format ``(input code, output code, type, value)``.

        """
        super(IOTableBackend, self).write(products, process=False)
        print("Starting IO table write")
        num_products = len(products)

        # Create geomapping array
        arr = np.zeros(
            (num_products, ),
            dtype=self.dtype_fields_geomapping + self.base_uncertainty_fields
        )

        print("Writing geomapping")
        for index, row in enumerate(sorted(products.values(),
                                           key=lambda x: x.get('key'))):
            arr[index] = (
                mapping[row['key']],
                geomapping[row['location'] or config.global_location],
                MAX_INT_32, MAX_INT_32,
                0, 1, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, False
            )

        with open(self.filepath_geomapping(), "wb") as f:
            pickle.dump(arr, f, protocol=pickle.HIGHEST_PROTOCOL)

        dependents = set()

        print("Creating array - this will take a while...")
        step = 5000000
        arr = np.zeros((step,), dtype=self.dtype)

        for index, row in enumerate(exchanges):
            if index and not index % 1000000:
                print("On exchange number {}".format(index))
            if index and not index % step:
                # Add another `step` rows
                arr = np.hstack((
                    arr,
                    np.zeros((step,), dtype=self.dtype)
                ))

            dependents.add(row[0][0])

            try:
                arr[index] = (
                    mapping[row[0]],
                    mapping[row[1]],
                    MAX_INT_32, MAX_INT_32,
                    TYPE_DICTIONARY[row[2]],
                    0,  # No uncertainty for IO... :(
                    row[3], row[3],
                    np.NaN, np.NaN, np.NaN, np.NaN, row[3] < 0
                )
            except KeyError:
                raise UnknownObject(("Exchange between {} and {} is invalid "
                    "- one of these objects is unknown (i.e. doesn't exist "
                    "as a process dataset)"
                    ).format(row[0], row[1])
                )

        for index2, obj in enumerate(products):
            if index and not index % step:
                arr = np.hstack((
                    arr,
                    np.zeros((step,), dtype=self.dtype)
                ))

            arr[index + index2] = (
                mapping[obj], mapping[obj],
                MAX_INT_32, MAX_INT_32,
                TYPE_DICTIONARY['production'],
                0, 1, 1, np.NaN, np.NaN, np.NaN, np.NaN, False
            )

        databases[self.name]['depends'] = sorted(dependents.difference({self.name}))
        databases[self.name]['processed'] = datetime.datetime.now().isoformat()
        databases.flush()

        # Trim arr
        print("Trimming array")
        arr = arr[np.where(arr["row"] == MAX_INT_32)]

        print("Writing array")
        with open(self.filepath_processed(), "wb") as f:
            pickle.dump(arr, f, protocol=pickle.HIGHEST_PROTOCOL)

    def process(self):
        """No-op; no intermediate data to process"""
        return
