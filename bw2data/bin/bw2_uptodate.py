#!/usr/bin/env python
# encoding: utf-8
"""Brightway2 updating made simple.

Usage:
  bw2-uptodate
  bw2-uptodate -l | --list
  bw2-uptodate -h | --help
  bw2-uptodate --version

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
    from bw2data import config, Updates
from bw2data.colors import Fore, init, deinit


class UpdaterInterface(object):
    def needed(self):
        return Updates.check_status(False)

    def list(self):
        updates_needed = self.needed()

        if not updates_needed:
            print(Fore.GREEN + "\n*** Brightway2 is up to date! ***\n")
        else:
            print(Fore.RED + "\n*** Updates found ***")
            for update in updates_needed:
                print(Updates.explain(update))
            print(Fore.RED + "\n*** Action needed ***" + Fore.RESET + \
                "\nPlease run " + Fore.BLUE + "bw2-uptodate.py\n")

    def update(self, confirm=True):
        updates_needed = self.needed()

        if updates_needed:
            print(Fore.GREEN + "\nThe following updates will be applied:\n")
            for update in updates_needed:
                print(Updates.explain(update))
            if confirm:
                confirmation = raw_input("\nType '" + Fore.MAGENTA  + "y" + \
                    Fore.RESET + "'to confirm, " + Fore.RED + "anything else" + \
                    Fore.RESET + " to cancel: "
                )
                if confirmation.strip() != 'y':
                    print(Fore.MAGENTA + "\n*** Upgrade canceled ***\n")
                    sys.exit(0)

            for update in updates_needed:
                Updates.do_update(update)
        else:
            print(Fore.GREEN + "\n*** Brightway2 is up to date! ***\n")


def main():
    try:
        init(autoreset=True)
        config.create_basic_directories()
        args = docopt(__doc__, version='Brightway2 up to date 0.1')
        updater_interface = UpdaterInterface()
        if args['--list'] or args['-l']:
            updater_interface.list()
        else:
            updater_interface.update()
    except:
        deinit()
        raise
    finally:
        deinit()


if __name__ == "__main__":
    main()
