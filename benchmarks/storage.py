"""Compare the compressed JSON ``data`` columns with the pickles they replace. Not run in CI.

Writes a synthetic database (by default 20,000 activities and 500,000 exchanges) in a
temporary project, then times ``Database.write()``, ``process()`` and reading every
exchange, and reports the file size. The same steps then run with the rows rewritten as
pickles, which is how bw2data stored them before.

Reading every exchange is timed on the raw column, decoded as each version does it: the
compressed rows with ``CompressedJSONField``, the pickles with ``pickle.loads``. ``process()``
can only be run by this version, which reads pickles with the restricted unpickler
(``codec.safe_pickle_loads``), slower than the ``pickle.loads`` of earlier versions; its
time for pickles is therefore an upper bound. The zstd dictionary is deleted before the
size of the file with pickles is measured. Usage::

    python benchmarks/storage.py [--activities N] [--exchanges-per-activity M]

Uses a temporary directory as ``BRIGHTWAY2_DIR``, so no existing project is touched.
"""

import argparse
import os
import pickle
import random
import tempfile
import time
from contextlib import contextmanager

COMMENTS = [
    "Calculated from the annual production volume of the market and the share of each supplier. "
    * 3,
    "Literature value, adjusted to the reference year with the producer price index. " * 2,
    "Estimated from similar processes in the same region; see the general comment. " * 2,
]
PROPERTIES = {
    "carbon content, fossil": {"amount": 0.0, "unit": "dimensionless"},
    "carbon content, non-fossil": {"amount": 0.42, "unit": "dimensionless"},
    "dry mass": {"amount": 0.89, "unit": "kg"},
    "water in wet mass": {"amount": 0.11, "unit": "kg"},
}


def synthetic_data(name: str, activities: int, per_activity: int) -> dict:
    rng = random.Random(42)
    data = {}
    for i in range(activities):
        exchanges = [
            {
                "input": (name, str(rng.randrange(activities))),
                "amount": rng.lognormvariate(0, 2),
                "type": "technosphere",
                "uncertainty type": 2,
                "loc": rng.gauss(0, 1),
                "scale": 0.1 + rng.random() / 10,
                "pedigree": {
                    "reliability": rng.randint(1, 5),
                    "completeness": rng.randint(1, 5),
                    "temporal correlation": rng.randint(1, 5),
                    "geographical correlation": rng.randint(1, 5),
                    "further technological correlation": rng.randint(1, 5),
                },
                "comment": rng.choice(COMMENTS),
                "properties": PROPERTIES,
                "name": f"product {rng.randrange(2000)}",
                "unit": "kilogram",
            }
            for _ in range(per_activity)
        ]
        data[(name, str(i))] = {
            "name": f"activity {i}",
            "reference product": f"product {i % 2000}",
            "unit": "kilogram",
            "location": rng.choice(["CH", "DE", "GLO", "RoW", "US"]),
            "type": "process",
            "comment": rng.choice(COMMENTS),
            "classifications": [("ISIC rev.4 ecoinvent", f"{rng.randrange(9999)}:Some sector")],
            "exchanges": exchanges,
        }
    return data


@contextmanager
def timed(label: str, results: dict):
    start = time.perf_counter()
    yield
    results[label] = time.perf_counter() - start


def measure(sqlite3_lci_db, db, results: dict, decode, pickles: bool = False) -> None:
    """``decode`` turns a stored value and the four key columns into the dataset. ``pickles``: the rows are pickles,
    so the file has to be measured without the zstd dictionary, which earlier versions
    didn't have."""
    with timed("process()", results):
        db.process()
    with timed("read all exchanges", results):
        cursor = sqlite3_lci_db.db.connection().execute(
            "SELECT data, input_database, input_code, output_database, output_code "
            "FROM exchangedataset"
        )
        try:
            for value, *keys in cursor:
                decode(value, *keys)
        finally:
            cursor.close()
    if pickles:
        sqlite3_lci_db.execute_sql("DELETE FROM zstddictionary")
    sqlite3_lci_db.execute_sql("VACUUM")
    results["file size (MB)"] = os.path.getsize(sqlite3_lci_db.db.database) / 1e6


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--activities", type=int, default=20_000)
    parser.add_argument("--exchanges-per-activity", type=int, default=25)
    args = parser.parse_args()

    os.environ["BRIGHTWAY2_DIR"] = tempfile.mkdtemp(prefix="bw2data-benchmark-")
    import bw2data as bd
    from bw2data.backends import sqlite3_lci_db
    from bw2data.backends.schema import ActivityDataset, ExchangeDataset, add_exchange_keys

    bd.projects.set_current("storage benchmark")
    data = synthetic_data("bench", args.activities, args.exchanges_per_activity)
    db = bd.Database("bench")

    compressed = {}
    with timed("write()", compressed):
        db.write(data, process=False, searchable=False)
    measure(
        sqlite3_lci_db,
        db,
        compressed,
        # `input` and `output` aren't in the stored value; the pickles below hold them
        lambda value, *keys: add_exchange_keys(ExchangeDataset.data.python_value(value), *keys),
    )

    # The same rows as pickles, as written by earlier versions (``PickleField``)
    pickled = {}
    with timed("write()", pickled):
        with sqlite3_lci_db.atomic():
            for model, table in ((ActivityDataset, "activitydataset"), (ExchangeDataset, "exchangedataset")):
                rows = [(pickle.dumps(r.data, protocol=4), r.id) for r in model.select()]
                sqlite3_lci_db.db.connection().executemany(
                    f"UPDATE {table} SET data = ? WHERE id = ?", rows
                )
    pickled["write()"] = None  # not comparable: decodes and re-encodes every row
    measure(sqlite3_lci_db, db, pickled, lambda value, *keys: pickle.loads(bytes(value)), pickles=True)

    exchanges = args.activities * args.exchanges_per_activity
    print(f"{args.activities:,} activities, {exchanges:,} exchanges")
    print(f"{'':24}{'compressed JSON':>18}{'pickle':>12}")
    for label in compressed:
        values = [compressed[label], pickled[label]]
        cells = [f"{v:12.2f}" if v is not None else f"{'-':>12}" for v in values]
        print(f"{label:24}{cells[0]:>18}{cells[1]:>12}")
    print("process() with pickles uses the restricted unpickler of this version (upper bound)")


if __name__ == "__main__":
    main()
