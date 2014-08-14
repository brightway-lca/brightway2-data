# -*- coding: utf-8 -*-
from __future__ import print_function
from . import Database, databases, Method, methods, config, Weighting, \
    weightings, Normalization, normalizations
from .colors import Fore, safe_colorama
from .ia_data_store import abbreviate
from .units import normalize_units
from .utils import activity_hash, recursive_str_to_unicode
import os
import pprint
import progressbar
import re
import warnings

try:
    import cPickle as pickle
except ImportError:
    import pickle

hash_re = re.compile("^[a-zA-Z0-9]{32}$")
is_hash = lambda x: bool(hash_re.match(x))

UPTODATE_WARNING = Fore.RED + "\n\nYour data needs to be updated." + Fore.RESET \
    + " Please run the following program on the command line:\n\n\t" + \
    Fore.BLUE + "bw2-uptodate.py\n" + Fore.RESET

widgets = [
    progressbar.SimpleProgress(sep="/"), " (",
    progressbar.Percentage(), ') ',
    progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
    progressbar.ETA()
]


class Updates(object):
    UPDATES = {
        '0.10 units restandardization': {
            "method": "units_renormalize",
            "explanation": Fore.GREEN + "0.10 units restandardization:" + Fore.RESET + """\n\tBrightway2 tries to normalize units so that they are consistent from machine to machine, and person to person. For example, ``m2a`` is changed to ``square meter-year``. This update adds more data normalizations, and needs to updates links across databases."""},
        # '0.11 reprocess IA methods': {
        #     "method": "reprocess_all_lcia",
        #     "explanation": Fore.GREEN + "0.11 reprocess IA methods" + Fore.RESET + """\n\t0.11 changed the format for processed IA methods, and the algorithm used to shorten IA method names."""},
        "0.12 reprocess inventory databases": {
            'method': "redo_all_databases_0_12",
            "explanation": Fore.GREEN + "0.12 reprocess inventory databases" + Fore.RESET + "\n\t0.12 changed the algorithm to create filenames based on database and LCIA method names, to make sure they don't contain illegal characters."},
        "0.12 reprocess IA databases": {
            "method": "reprocess_all_lcia",
            "explanation": Fore.GREEN + "0.12 reprocess IA databases" + Fore.RESET + "\n\t0.12 changed the algorithm to create filenames based on database and LCIA method names, to make sure they don't contain illegal characters."},
        "0.14 update biosphere hashes": {
            "method": "update_biosphere_hashes",
            "explanation": Fore.GREEN + "0.14 update biosphere hashes" + Fore.RESET + "\n\tPrevious upgrades didn't correctly apply the new hashing algorithm to the biosphere database. This update fixes the ``config.biosphere`` database, all of its children, and all LCIA methods."},
        "0.16 decode inventory databases": {
            'method': "decode_inventory_databases_0_16",
            'explanation': Fore.GREEN + "0.16 decode inventory databases" + Fore.RESET + "\n\tAll inventory database strings should be unicode, not byte strings. However, as Brightway2 doesn't know how the strings are encoded, you will need to fix this manually. This update only gives instructions; it doesn't actually do anything."},
        "1.0 reprocess all objects": {
            'method': "reprocess_all_1_0",
            'explanation': Fore.GREEN + "1.0 relaxed previous restrictions on what had to be included in Databases, IA methods, etc. These objects need to be reprocessed to put in default values."},
    }

    @staticmethod
    def explain(key):
        return Updates.UPDATES[key]['explanation']

    @staticmethod
    def do_update(key):
        method = getattr(Updates, Updates.UPDATES[key]['method'])
        method()
        config.p['updates'][key] = True
        config.save_preferences()

    @staticmethod
    def check_status(verbose=True):
        """Check if updates need to be applied.

        Returns:
            List of needed updates (strings), if any.

        """
        updates = []

        # Remove in 1.0
        if "upgrades" in config.p:
            config.p['updates'] = config.p['upgrades']
            del config.p['upgrades']

        if "updates" not in config.p:
            config.p['updates'] = {key: True for key in Updates.UPDATES}
            config.save_preferences()
        else:
            updates = sorted([key for key in Updates.UPDATES if not config.p['updates'].get(key)])
        if updates and verbose:
            with safe_colorama():
                warnings.warn(UPTODATE_WARNING)
        return updates

    @staticmethod
    def redo_all_databases_0_12():
        def load_data_old_filename(name, version):
            return pickle.load(open(os.path.join(
                config.dir,
                u"intermediate",
                name + u"." + unicode(version) + u".pickle"
            ), "rb"))

        print("Updating all LCI databases")

        pbar = progressbar.ProgressBar(
            widgets=widgets,
            maxval=len(databases)
        ).start()

        for index, name in enumerate(databases):
            db = Database(name)
            data = load_data_old_filename(name, db.version)
            db.write(data)
            db.process()

            databases[name]['filename'] = db.filename
            databases.flush()

            pbar.update(index)

        pbar.finish()

    @staticmethod
    def decode_inventory_databases_0_16():
        print(Fore.GREEN + u"Updating LCIA methods" + Fore.RESET)

        pbar = progressbar.ProgressBar(
            widgets=widgets,
            maxval=len(methods)
        ).start()

        for index, name in enumerate(methods):
            method = Method(name)
            data = recursive_str_to_unicode(method.load())
            method.write(data)
            method.process()

            pbar.update(index)

        pbar.finish()

        print(u"Processing databases. Anything other than '" +
              Fore.RED + u"n" + Fore.RESET + u"' is interpreted as yes.")

        skipped = []

        for name in databases:
            database = Database(name)
            data = database.load()
            windows = raw_input(u"Was the database " + Fore.BLUE +
                                database.name + Fore.RESET + u" imported from XML files? [Y/n]")
            try:
                if windows.lower() == "n":
                    data = recursive_str_to_unicode(data, "latin-1")
                else:
                    data = recursive_str_to_unicode(data)
            except UnicodeDecodeError:
                skipped.append(database.name)
                print(u"Can't handle the encoding of database " + Fore.BLUE + database.name +
                      Fore.RESET + u". It is skipped; you will have to fix it manually")
            database.write(data)
            database.process()

        if skipped:
            print(Fore.RED + u"\nERROR" + Fore.RESET + u": Some databases must be fixed manually:\n\t" + u"\n\t".join(skipped))

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

                pbar = progressbar.ProgressBar(
                    widgets=widgets,
                    maxval=len(meta)
                ).start()

                for index, key in enumerate(meta):
                    obj = klass(key)
                    obj.process()
                    # Free memory
                    obj = None
                    config.cache = {}
                    pbar.update(index)

                pbar.finish()

    @staticmethod
    def reprocess_all_lcia():
        """0.11: Change name hashing function from random characters (!?) to MD5 hash. Need to update abbreviations and rewrite all data.

        0.12: Make sure strings are sanitized to be able to be used in filenames. Need to update abbreviations and rewrite all data."""
        LCIA = [
            (methods, Method, "LCIA methods"),
            (weightings, Weighting, "LCIA weightings"),
            (normalizations, Normalization, "LCIA normalizations")
        ]

        for (meta, klass, name) in LCIA:
            if meta.list:
                print("Updating all %s" % name)

                pbar = progressbar.ProgressBar(
                    widgets=widgets,
                    maxval=len(meta)
                ).start()

                for index, key in enumerate(meta):
                    obj = klass(key)
                    data = obj.load()
                    meta[key]['abbreviation_old'] = \
                        meta[key]['abbreviation']
                    meta[key]['abbreviation'] = abbreviate(key)
                    meta.flush()
                    obj.write(data)
                    obj.process()
                    pbar.update(index)

                pbar.finish()

    @staticmethod
    def units_renormalize():
        """Renormalize some units, making many activity datasets with hash ids change."""
        db_versions = {name: databases[name]['version'] for name in databases.list}

        try:
            mapping = {}

            print("Updating inventory databases.\nFirst pass: Checking process IDs")

            pbar = progressbar.ProgressBar(
                widgets=widgets,
                maxval=len(databases.list)
            ).start()

            for index, database in enumerate(databases.list):
                db = Database(database)
                db_data = db.load()
                for key, ds in db_data.iteritems():
                    old_hash = (database, activity_hash(ds))
                    ds['unit'] = normalize_units(ds.get('unit', ''))
                    if key[1] != old_hash:
                        continue
                    new_hash = (database, activity_hash(ds))
                    if new_hash != old_hash:
                        mapping[old_hash] = new_hash

                for key, ds in db_data.iteritems():
                    if key in mapping:
                        db_data[mapping[key]] = db_data[key]
                        del db_data[key]

                db.write(db_data)
                pbar.update(index)

            pbar.finish()

            print("Second pass: Fixing links...")

            pbar = progressbar.ProgressBar(
                widgets=widgets,
                maxval=len(databases)
            ).start()

            for index, database in enumerate(databases.list):
                db = Database(database)
                db_data = db.load()
                for ds in db_data.values():
                    for exc in ds.get("exchanges", []):
                        if tuple(exc['input']) in mapping:
                            exc['input'] = mapping[tuple(exc['input'])]

                db.write(db_data)
                db.process()
                pbar.update(index)

            pbar.finish()

            print("Updating IA methods")

            pbar = progressbar.ProgressBar(
                widgets=widgets,
                maxval=len(methods)
            ).start()

            for index, method in enumerate(methods.list):
                m = Method(method)
                m_data = m.load()
                for row in m_data:
                    if row[0] in mapping:
                        row[0] = mapping[row[0]]

                m.write(m_data)
                m.process()
                pbar.update(index)

            pbar.finish()

        except:
            print("Oops, something went wrong. Reverting all changes...")
            for database in databases.list:
                Database(database).revert(db_versions[database])
            raise

    @staticmethod
    def update_biosphere_hashes():
        Updates.update_ids_new_hashes(config.biosphere)

    @staticmethod
    def update_ids_new_hashes(database):
        assert database in databases
        child_databases = [name for name in databases if database in databases[name]['depends']]
        # Version number of known good data
        previous_versions = {name: databases[name]['version']
                             for name in child_databases + [database]}
        database_obj = Database(database)
        data = database_obj.load()
        # Mapping from old values to new values
        mapping = {
            key: (database, activity_hash(data[key])) if is_hash(key[1]) else key
            for key in data
        }
        processed = []
        backup_methods = []

        try:
            print(Fore.GREEN + "Updating database and child databases..." + Fore.RESET)

            pbar = progressbar.ProgressBar(
                widgets=widgets,
                maxval=len(child_databases) + 1
            ).start()

            new_data = {
                (mapping[key] if key in mapping else key): value
                for key, value in data.iteritems()
            }
            database_obj.write(new_data)
            database_obj.process()
            processed.append(database)
            pbar.update(1)

            for index, child_name in enumerate(child_databases):
                child_obj = Database(child_name)
                child_data = child_obj.load()

                for data in child_data.values():
                    for exchange in data.get("exchanges", []):
                        if exchange['input'] in mapping:
                            exchange['input'] = mapping[exchange['input']]

                child_obj.write(child_data)
                child_obj.process()
                processed.append(child_name)
                pbar.update(index + 1)

            pbar.finish()

            if database == config.biosphere:
                print("Updating all IA methods")

                pbar = progressbar.ProgressBar(
                    widgets=widgets,
                    maxval=len(methods)
                ).start()

                for index, method in enumerate(methods):
                    method_obj = Method(method)
                    backup_name = method_obj.backup()
                    method_data = method_obj.load()

                    method_data = [
                        [mapping.get(obj[0], obj[0])] + list(obj[1:])
                        for obj in method_data
                    ]

                    method_obj.write(method_data)
                    method_obj.process()
                    backup_methods.append(backup_name)
                    pbar.update(index)

                pbar.finish()
        except:
            print("ERROR: Reverting all changes...")
            for name in processed:
                Database(name).revert(previous_versions[name])

            if backup_methods:
                print("The following method caused an error: {}".format(method))
                print("The following methods backups must be reverted manually:")
                pprint.pprint(backup_methods)

            raise
