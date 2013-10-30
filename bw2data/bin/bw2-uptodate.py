#!/usr/bin/env python
# encoding: utf-8
"""Brightway2 updating made simple.

Usage:
  bw2-uptodate.py
  bw2-uptodate.py --list
  bw2-uptodate.py -h | --help
  bw2-uptodate.py --version

Options:
  --list        List the updates needed, but don't do anything
  -h --help     Show this screen.
  --version     Show version.

"""
from __future__ import print_function
from docopt import docopt
import sys
import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from bw2data import config
    from bw2data.upgrades import *
from bw2data.colors import Fore, init, deinit

EXPLANATIONS = {
    "stats_array reformat": Fore.GREEN + "\nstats_array reformat:" + Fore.RESET + """
    Upgrading to the ``stats_arrays`` package changes the data format of both inventory databases and impact assessment methods.
    Read more about the stats_arrays data format: """ + Fore.BLUE + \
        "\n\thttps://stats_arrays.readthedocs.org/en/latest/\n" + Fore.RESET,
    "0.10 units restandardization": Fore.GREEN + "0.10 units restandardization:" + Fore.RESET + """
    Brightway2 tries to normalize units so that they are consistent from machine to machine, and person to person. For example, ``m2a`` is changed to ``square meter-year``. This update adds more data normalizations, and needs to updates links across databases.""",
}


class Updater(object):
    def needed(self):
        try:
            import stats_arrays
        except ImportError:
            warnings.warn(STATS_ARRAY_WARNING)
            sys.exit(0)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            updates_needed = check_status()
        return updates_needed

    def list(self):
        updates_needed = self.needed()
        if not updates_needed:
            print(Fore.GREEN + "\n*** Brightway2 is up to date! ***\n")
        else:
            print(Fore.RED + "\n*** Updates found ***")
            for update in updates_needed:
                print(EXPLANATIONS[update])
            print(Fore.RED + "\n*** Action needed ***" + Fore.RESET + \
                "\nPlease run " + Fore.BLUE + "bw2-uptodate.py\n")

    def update(self, confirm=True):
        updates_needed = self.needed()

        if updates_needed:
            print(Fore.GREEN + "\nThe following upgrades will be applied:\n")
            for update in updates_needed:
                print(EXPLANATIONS[update])
            if confirm:
                confirmation = raw_input("\nType '" + Fore.MAGENTA  + "y" + \
                    Fore.RESET + "'to confirm, " + Fore.RED + "anything else" + \
                    Fore.RESET + " to cancel: "
                )
                if confirmation.strip() != 'y':
                    print(Fore.MAGENTA + "\n*** Upgrade canceled ***\n")
                    sys.exit(0)

            if "stats_array reformat" in updates_needed:
                convert_from_stats_toolkit()
                config.p["upgrades"]["stats_array reformat"] = True
            if "0.10 units restandardization" in updates_needed:
                units_renormalize()
                config.p["upgrades"]["0.10 units restandardization"] = True
            config.save_preferences()
        else:
            print(Fore.GREEN + "\n*** Brightway2 is up to date! ***\n")


if __name__ == "__main__":
    try:
        init(autoreset=True)
        config.create_basic_directories()
        args = docopt(__doc__, version='Brightway2 up to date 0.1')
        updater = Updater()
        if args['--list']:
            updater.list()
        else:
            updater.update()
    except:
        deinit()
        raise
    finally:
        deinit()
