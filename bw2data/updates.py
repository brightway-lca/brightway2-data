import os
import pickle
import re
import shutil
import sqlite3
import warnings
from pathlib import Path

import numpy as np
import pyprind
from bw_processing import safe_filename

from . import (
    Database,
    Method,
    Normalization,
    Weighting,
    databases,
    methods,
    normalizations,
    preferences,
    projects,
    weightings,
)
from .backends import sqlite3_lci_db

hash_re = re.compile("^[a-zA-Z0-9]{32}$")
is_hash = lambda x: bool(hash_re.match(x))

UPTODATE_WARNING = "\n\nYour data needs to be updated. Please run the following program on the command line:\n\n\tbw2-uptodate\n"

UPDATE_ACTIVITYDATASET = """
BEGIN;
DROP INDEX IF EXISTS "activitydataset_key";
ALTER TABLE ActivityDataset rename to AD_old;
CREATE TABLE "activitydataset" (
    "id" INTEGER NOT NULL PRIMARY KEY,
    "database" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "data" BLOB NOT NULL,
    "location" TEXT,
    "name" TEXT,
    "product" TEXT,
    "type" TEXT
);
INSERT INTO ActivityDataset ("database", "code", "data", "location", "name", "product", "type")
    SELECT substr(key, 0, instr(key, '⊡')),
        substr("key", instr("key", '⊡') + 1),
        "data",
        "location",
        "name",
        "product",
        "type"
    FROM AD_old;
CREATE UNIQUE INDEX "activitydataset_key" ON "activitydataset" ("database", "code");
DROP TABLE AD_old;
COMMIT;
"""

UPDATE_EXCHANGEDATASET = """
BEGIN;
DROP INDEX IF EXISTS "exchangedataset_database";
DROP INDEX IF EXISTS "exchangedataset_input";
DROP INDEX IF EXISTS "exchangedataset_output";
ALTER TABLE ExchangeDataset rename to ED_old;
CREATE TABLE "exchangedataset" (
    "id" INTEGER NOT NULL PRIMARY KEY,
    "data" BLOB NOT NULL,
    "input_database" TEXT NOT NULL,
    "input_code" TEXT NOT NULL,
    "output_database" TEXT NOT NULL,
    "output_code" TEXT NOT NULL,
    "type" TEXT NOT NULL
);
INSERT INTO ExchangeDataset ("data", "input_database", "input_code", "output_database", "output_code", "type")
    SELECT "data",
        substr("input", 0, instr("input", '⊡')),
        substr("input", instr("input", '⊡') + 1),
        substr("output", 0, instr("output", '⊡')),
        substr("output", instr("output", '⊡') + 1),
        "type"
    FROM ED_old;
CREATE INDEX "exchangedataset_input" ON "exchangedataset" ("input_database", "input_code");
CREATE INDEX "exchangedataset_output" ON "exchangedataset" ("output_database", "output_code");
DROP TABLE ED_old;
COMMIT;
"""


class Updates:
    UPDATES = {
        "2.0 schema change": {
            "method": "schema_change_20_compound_keys",
            "explanation": "",
            "automatic": True,
        },
        "2.0-2 database search directories": {
            "method": "database_search_directories_20",
            "automatic": True,
            "explanation": "",
        },
        "2.3 processed data format": {
            "method": "processed_data_format_change_23",
            "automatic": True,
            "explanation": "",
        },
        "4.0 new processed format": {
            "method": "expire_all_processed_data_40",
            "automatic": True,
            "explanation": "bw2data 4.0 release requires all database be reprocessed",
        },
        "4.0 migrations filename change": {
            "method": "fix_migrations_filename",
            "automatic": True,
            "explanation": "bw2data 4.0 release requires migrations filename changes",
        },
    }

    @classmethod
    def explain(cls, key):
        return cls.UPDATES[key]["explanation"]

    @classmethod
    def do_update(cls, key):
        method = getattr(cls, cls.UPDATES[key]["method"])
        method()
        preferences["updates"][key] = True
        preferences.flush()

    @classmethod
    def check_status(cls, verbose=True):
        """Check if updates need to be applied.

        Returns:
            List of needed updates (strings), if any.

        """
        cls.set_initial_updates()
        updates = sorted(
            [
                key
                for key in cls.UPDATES
                if not preferences["updates"].get(key)
                and not cls.UPDATES[key]["automatic"]
            ]
        )
        if updates and verbose:
            warnings.warn(UPTODATE_WARNING)
        return updates

    @classmethod
    def set_initial_updates(cls):
        if "updates" in preferences:
            return
        SQL = "PRAGMA table_info(activitydataset)"
        with sqlite3.connect(sqlite3_lci_db.db.database) as conn:
            column_names = {x[1] for x in conn.execute(SQL)}
        if "code" in column_names:
            preferences["updates"] = {key: True for key in cls.UPDATES}
        else:
            preferences["updates"] = {}

    @classmethod
    def check_automatic_updates(cls):
        """Get list of automatic updates to be applied"""
        cls.set_initial_updates()
        return sorted(
            [
                key
                for key in cls.UPDATES
                if not preferences["updates"].get(key) and cls.UPDATES[key]["automatic"]
            ]
        )

    @classmethod
    def reprocess_all_1_0(cls):
        """1.0: Reprocess all to make sure default 'loc' value inserted when not specified."""
        cls._reprocess_all()

    @classmethod
    def schema_change_20_compound_keys(cls):
        with sqlite3.connect(sqlite3_lci_db.db.database) as conn:
            print("Update ActivityDataset table schema and data")
            conn.executescript(UPDATE_ACTIVITYDATASET)
            print("Updating ExchangeDataset table schema and data")
            conn.executescript(UPDATE_EXCHANGEDATASET)
            print("Finished with schema change")

    @classmethod
    def database_search_directories_20(cls):
        shutil.rmtree(projects.request_directory("whoosh"))
        for db in databases:
            if databases[db].get("searchable"):
                databases[db]["searchable"] = False
                print("Reindexing database {}".format(db))
                Database(db).make_searchable()

    @classmethod
    def processed_data_format_change_23(cls):
        processed_dir = projects.dir / "processed"
        for filename in os.listdir(processed_dir):
            fp = processed_dir / filename
            if fp.is_dir():
                continue
            if not filename.endswith(".pickle"):
                continue
            np.save(fp[:-7] + ".npy", pickle.load(open(fp, "rb")), allow_pickle=False)

    @classmethod
    def expire_all_processed_data_40(cls):
        cls._reprocess_all()

    @classmethod
    def fix_migrations_filename(cls):
        """ "Fix migration data filenames to use shorter hash.

        See https://github.com/brightway-lca/brightway2-io/issues/115"""
        try:
            import bw2io as bi
        except ImportError:
            print("`bw2io` not installed; not updating `migrations` filenames")
            return

        for name in bi.migrations:
            current = Path(
                projects.request_directory("migrations")
                / (safe_filename(name, full=True) + ".json")
            )
            assert current.is_file()
            target = Path(
                projects.request_directory("migrations")
                / (safe_filename(name) + ".json")
            )
            current.replace(target)

    @classmethod
    def _reprocess_all(cls):
        objects = [
            (methods, Method, "LCIA methods"),
            (weightings, Weighting, "LCIA weightings"),
            (normalizations, Normalization, "LCIA normalizations"),
            (databases, Database, "LCI databases"),
        ]

        for (meta, klass, name) in objects:
            if meta.list:
                print("Updating all %s" % name)

                pbar = pyprind.ProgBar(
                    len(meta), title="Brightway2 {} objects:".format(name), monitor=True
                )

                for index, key in enumerate(meta):
                    obj = klass(key)
                    obj.process()
                    # Free memory
                    obj = None

                    pbar.update()
                print(pbar)
