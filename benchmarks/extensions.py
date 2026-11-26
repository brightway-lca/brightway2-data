"""Time read-only LCI extensions against the same database in the project's own file. Not run
in CI.

Writes a synthetic database (by default 20,000 activities and 500,000 exchanges, the same data
as ``benchmarks/storage.py``) in a project and exports it as an extension. Then, once in a
project which attaches the extension and once in the project which has the database in its own
file, times:

* ``len(Database(...))`` and reading 1,000 nodes with ``get_node``, which go through the overlay
  views when an extension is attached;
* ``upstream()`` of 200 nodes (the exchanges whose input is the node);

  these three as the fastest of three runs, as the first one may read files which are not in
  the operating system's cache yet;
* writing a database of 10,000 datasets in the project, each linking to the benchmark database,
  with the Python-level guards on, and deleting it again with ``Database.delete``;
* getting the processed datapackage: read from the extension file, or processed in the
  project.

Also reports the time to export and to attach, and the size of the extension file. Usage::

    python benchmarks/extensions.py [--activities N] [--exchanges-per-activity M]

Uses a temporary directory as ``BRIGHTWAY2_DIR``, so no existing project is touched.
"""

import argparse
import os
import random
import tempfile
import time
from contextlib import contextmanager

from storage import synthetic_data


@contextmanager
def timed(label: str, results: dict):
    start = time.perf_counter()
    yield
    results[label] = time.perf_counter() - start


def best_of(label: str, results: dict, function, repeat: int = 3) -> None:
    """The fastest of ``repeat`` runs: the first run reads a file which may not be in the
    operating system's cache yet, e.g. right after attaching."""
    times = []
    for _ in range(repeat):
        start = time.perf_counter()
        function()
        times.append(time.perf_counter() - start)
    results[label] = min(times)


def foreground(name: str, linked: str, count: int, activities: int) -> dict:
    """``count`` datasets of database ``name``, each with one exchange from ``linked``."""
    rng = random.Random(7)
    return {
        (name, str(i)): {
            "name": f"foreground {i}",
            "unit": "kilogram",
            "location": "CH",
            "type": "process",
            "exchanges": [
                {"input": (name, str(i)), "amount": 1, "type": "production"},
                {
                    "input": (linked, str(rng.randrange(activities))),
                    "amount": rng.random(),
                    "type": "technosphere",
                },
            ],
        }
        for i in range(count)
    }


def measure(bd, name: str, activities: int, results: dict) -> None:
    rng = random.Random(1)
    codes = [str(rng.randrange(activities)) for _ in range(1_000)]
    best_of("len(Database)", results, lambda: len(bd.Database(name)))
    best_of(
        "get_node x 1,000", results, lambda: [bd.get_node(database=name, code=c) for c in codes]
    )
    nodes = [bd.get_node(database=name, code=code) for code in codes[:200]]
    best_of("upstream() x 200", results, lambda: [list(node.upstream()) for node in nodes])
    data = foreground("foreground", name, 10_000, activities)
    with timed("write 10,000 linked", results):
        bd.Database("foreground").write(data, process=False, searchable=False)
    with timed("Database.delete", results):
        bd.Database("foreground").delete(warn=False)
    with timed("processed datapackage", results):
        bd.Database(name).datapackage()


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--activities", type=int, default=20_000)
    parser.add_argument("--exchanges-per-activity", type=int, default=25)
    args = parser.parse_args()

    directory = tempfile.mkdtemp(prefix="bw2data-benchmark-")
    os.environ["BRIGHTWAY2_DIR"] = directory
    import bw2data as bd

    name = "bench"
    bd.projects.set_current("in the project")
    data = synthetic_data(name, args.activities, args.exchanges_per_activity)
    bd.Database(name).write(data, process=False, searchable=False)
    del data
    bd.Database(name).process()
    in_project = {}
    path = os.path.join(directory, "bench-extension.db")
    with timed("export", in_project):
        bd.projects.export_extension(path, name)
    export_time = in_project.pop("export")
    # Processed in the project, as there is no extension
    bd.Database(name).metadata["dirty"] = True
    bd.Database(name).metadata.save()
    measure(bd, name, args.activities, in_project)

    bd.projects.set_current("with the extension")
    in_extension = {}
    start = time.perf_counter()
    bd.projects.attach_extension(path)
    attach_time = time.perf_counter() - start
    measure(bd, name, args.activities, in_extension)

    exchanges = args.activities * args.exchanges_per_activity
    print(f"{args.activities:,} activities, {exchanges:,} exchanges")
    print(f"export {export_time:.2f} s, attach {attach_time:.2f} s, ", end="")
    print(f"extension file {os.path.getsize(path) / 1e6:.1f} MB")
    print(f"{'(seconds)':28}{'own file':>12}{'extension':>12}")
    for label in in_project:
        print(f"{label:28}{in_project[label]:12.3f}{in_extension[label]:12.3f}")
    print(
        "processed datapackage: processed in the project (own file) or read from the "
        "extension file"
    )


if __name__ == "__main__":
    main()
