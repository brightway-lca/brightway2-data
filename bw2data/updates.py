# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from . import Database, databases, Method, methods, Weighting, \
    weightings, Normalization, normalizations, preferences
from .backends.peewee import sqlite3_lci_db
from .ia_data_store import abbreviate
from .utils import recursive_str_to_unicode
import os
import pprint
import pyprind
import re
import sqlite3
import warnings

try:
    import cPickle as pickle
except ImportError:
    import pickle

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


class Updates(object):
    UPDATES = {
        "2.0 schema change": {
            'method': 'schema_change_20_compound_keys',
            'explanation': "",
            'automatic': True
        },
        # Update to 3.2 biosphere
    }

    @staticmethod
    def explain(key):
        return Updates.UPDATES[key]['explanation']

    @staticmethod
    def do_update(key):
        method = getattr(Updates, Updates.UPDATES[key]['method'])
        method()
        preferences['updates'][key] = True
        preferences.flush()

    @staticmethod
    def check_status(verbose=True):
        """Check if updates need to be applied.

        Returns:
            List of needed updates (strings), if any.

        """
        updates = []

        if "updates" not in preferences:
            preferences['updates'] = {key: True for key in Updates.UPDATES}
        else:
            updates = sorted([key for key in Updates.UPDATES
                              if not preferences['updates'].get(key)
                              and not Updates.UPDATES[key]['automatic']])
        if updates and verbose:
            warnings.warn(UPTODATE_WARNING)
        return updates

    @staticmethod
    def check_automatic_updates():
        """Get list of automatic updates to be applied"""
        if "updates" not in preferences:
            preferences['updates'] = {
                key: True
                for key in Updates.UPDATES
                if key != "2.0 schema change"
            }
        return sorted([key for key in Updates.UPDATES
                          if not preferences['updates'].get(key)
                          and Updates.UPDATES[key]['automatic']])

    @staticmethod
    def reprocess_all_1_0():
        """1.0: Reprocess all to make sure default 'loc' value inserted when not specified."""
        objects = [
            (methods, Method, "LCIA methods"),
            (weightings, Weighting, "LCIA weightings"),
            (normalizations, Normalization, "LCIA normalizations"),
            (databases, Database, "LCI databases"),
        ]

        for (meta, klass, name) in objects:
            if meta.list:
                print("Updating all %s" % name)

                pbar = pyprind.ProgBar(len(meta), title="Brightway2 {} objects:".format(name), monitor=True)

                for index, key in enumerate(meta):
                    obj = klass(key)
                    obj.process()
                    # Free memory
                    obj = None

                    pbar.update()
                print(pbar)

    @staticmethod
    def schema_change_20_compound_keys():
        with sqlite3.connect(sqlite3_lci_db.database) as conn:
            print("Update ActivityDataset table schema and data")
            conn.executescript(UPDATE_ACTIVITYDATASET)
            print("Updating ExchangeDataset table schema and data")
            conn.executescript(UPDATE_EXCHANGEDATASET)
            print("Finished with schema change")
