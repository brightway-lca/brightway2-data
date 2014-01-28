# -*- coding: utf-8 -*-
from . import Database, databases, Method, methods, config
from .colors import Fore, safe_colorama
from .ia_data_store import abbreviate
from .units import normalize_units
from .utils import activity_hash
import numpy as np
import progressbar
import sys
import warnings

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
        '0.11 reprocess IA methods': {
            "method": "reprocess_all_methods",
            "explanation": Fore.GREEN + "0.11 reprocess IA methods" + Fore.RESET + """\n\t0.11 changed the format for processed IA methods, and the algorithm used to shorten IA method names."""},
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

        # Remove in 0.12
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
    def reprocess_all_methods():
        """Change name hashing function from random characters (!?) to MD5 hash. Need to update abbreviations and rewrite all data."""
        print "Updating all LCIA methods"

        pbar = progressbar.ProgressBar(
            widgets=widgets,
            maxval=len(methods)
        ).start()

        for index, method_key in enumerate(methods):
            method = Method(method_key)
            method_data = method.load()
            methods[method_key]['abbreviation_old'] = \
                methods[method_key]['abbreviation']
            methods[method_key]['abbreviation'] = abbreviate(method_key)
            methods.flush()
            method.write(method_data)
            method.process()
            pbar.update(index)

        pbar.finish()

    @staticmethod
    def units_renormalize():
        """Renormalize some units, making many activity datasets with hash ids change."""
        db_versions = {name: databases[name]['version'] for name in databases.list}

        try:
            mapping = {}

            print "Updating inventory databases.\nFirst pass: Checking process IDs"

            pbar = progressbar.ProgressBar(
                widgets=widgets,
                maxval=len(databases.list)
            ).start()

            for index, database in enumerate(databases.list):
                db = Database(database)
                db_data = db.load()
                for key, ds in db_data.iteritems():
                    old_hash = (database, activity_hash(ds))
                    ds['unit'] = normalize_units(ds['unit'])
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

            print "Second pass: Fixing links..."

            pbar = progressbar.ProgressBar(
                widgets=widgets,
                maxval=len(databases)
            ).start()

            for index, database in enumerate(databases.list):
                db = Database(database)
                db_data = db.load()
                for ds in db_data.values():
                    for exc in ds['exchanges']:
                        if tuple(exc['input']) in mapping:
                            exc['input'] = mapping[tuple(exc['input'])]

                db.write(db_data)
                db.process()
                pbar.update(index)

            pbar.finish()

            print "Updating IA methods"

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
            print "Oops, something went wrong. Reverting all changes..."
            for database in databases.list:
                Database(database).revert(db_versions[database])
            raise
