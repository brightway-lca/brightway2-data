# -*- coding: utf-8 -*-
from . import Database, databases, Method, methods, config
from .colors import Fore, safe_colorama
from .units import normalize_units
from .utils import activity_hash
import numpy as np
import progressbar
import sys
import warnings


STATS_ARRAY_WARNING = "\n\nIt looks like you need to upgrade to the ``" + \
    Fore.GREEN + "stats_arrays" + Fore.RESET + \
    "`` package. This is a new statistical toolkit that replaces the deprecated ``" \
    + Fore.RED + "bw_stats_toolkit" + Fore.RESET + "``. Read more at """ + \
    Fore.BLUE + "https://bitbucket.org/cmutel/stats_arrays/." + Fore.RESET + \
    "\n\nTo do this, use `pip` (or whatever package manager you prefer) to install `stats_arrays`, e.g.:\n\n\t" \
    + Fore.MAGENTA + "pip install stats_arrays" + Fore.RESET + \
    "\n\nThen run the following program on the command line:\n\n\t" + \
    Fore.MAGENTA + "bw2-uptodate.py\n" + Fore.RESET

UPTODATE_WARNING = Fore.RED + "\n\nYour data needs to be updated." + Fore.RESET \
    + " Please run the following program on the command line:\n\n\t" + \
    Fore.BLUE + "bw2-uptodate.py\n" + Fore.RESET


def check_status():
    """Check if updates need to be applied.

    Returns:
        List of needed updates (strings), if any.

    """
    try:
        import stats_arrays
    except ImportError:
        with safe_colorama():
            warnings.warn(STATS_ARRAY_WARNING)
        if config._ipython:
            # ipython won't let us leave the shell...
            print "Please exit the IPython shell now"
            return
        else:
            sys.exit(0)
    updates = []
    if not databases.list:
        # First time setup - no upgrades needed
        # Setup function will populate config.p
        return []
    if "upgrades" not in config.p:
        config.p['upgrades'] = {}
        config.save_preferences()
    if not config.p['upgrades'].get("stats_array reformat", False):
        updates.append("stats_array reformat")
    if not config.p['upgrades'].get('0.10 units restandardization', False):
        updates.append('0.10 units restandardization')
    if updates:
        with safe_colorama():
            warnings.warn(UPTODATE_WARNING)
    return updates


def convert_from_stats_toolkit():
    """Convert all databases from ``bw_stats_toolkit`` to ``stats_arrays`` (https://bitbucket.org/cmutel/stats_arrays/)."""
    import stats_arrays as sa
    assert sa, "Must have `stats_arrays` package for this function"

    def update_exchange(exc):
        if exc.get('uncertainty type', None) is None:
            return exc
        elif 'loc' in exc:
            # Already updated
            return exc
        if 'sigma' in exc:
            exc['scale'] = exc['sigma']
            del exc['sigma']
        exc['loc'] = exc['amount']
        if exc['uncertainty type'] == sa.LognormalUncertainty.id:
            exc['negative'] = exc['amount'] < 0
            exc['loc'] = np.log(np.abs(exc['amount']))
        return exc

    print "Starting inventory conversion"
    for database in databases:
        print "Working on %s" % database
        db = Database(database)
        print "\t... loading ..."
        data = db.load()
        print "\t... converting ..."
        new_data = {}

        for index, (key, value) in enumerate(data.iteritems()):
            if 'exchanges' in value:
                value['exchanges'] = [update_exchange(exchange
                    ) for exchange in value['exchanges']]
            new_data[key] = value

        print "\t... writing ..."
        db.write(new_data)
        db.process()
    print "Inventory conversion finished\nStarting IA conversion"

    widgets = ['IA methods: ', progressbar.Percentage(), ' ',
               progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
               progressbar.ETA()]
    pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(methods.list)
                                   ).start()

    for index, name in enumerate(methods):
        method = Method(name)
        method.process()
        pbar.update(index)
    pbar.finish()
    print "Conversion finished"


def units_renormalize():
    """Renormalize some units, making many activity datasets with hash ids change."""
    db_versions = {name: databases[name]['version'] for name in databases.list}

    try:
        mapping = {}

        print "Updating inventory databases.\nFirst pass: Checking process IDs"

        widgets = [
            'Databases: ',
            progressbar.Percentage(),
            ' ',
            progressbar.Bar(marker=progressbar.RotatingMarker()),
            ' ',
            progressbar.ETA()
        ]
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

        widgets = [
            'Databases: ',
            progressbar.Percentage(),
            ' ',
            progressbar.Bar(marker=progressbar.RotatingMarker()),
            ' ',
            progressbar.ETA()
        ]
        pbar = progressbar.ProgressBar(
            widgets=widgets,
            maxval=len(databases.list)
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

        widgets = [
            'Methods: ',
            progressbar.Percentage(),
            ' ',
            progressbar.Bar(marker=progressbar.RotatingMarker()),
            ' ',
            progressbar.ETA()
        ]
        pbar = progressbar.ProgressBar(
            widgets=widgets,
            maxval=len(methods.list)
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
