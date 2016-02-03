#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
from __future__ import print_function, unicode_literals
from eight import *

from docopt import docopt
import sys
import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from bw2data import config, Updates


class UpdaterInterface(object):
    def needed(self):
        return Updates.check_status(False)

    def list(self):
        updates_needed = self.needed()

        if not updates_needed:
            print("\n*** Brightway2 is up to date! ***\n")
        else:
            print("\n*** Updates found ***")
            for update in updates_needed:
                print(Updates.explain(update))
            print("\n*** Action needed ***\nPlease run bw2-uptodate\n")

    def update(self, confirm=True):
        updates_needed = self.needed()

        if updates_needed:
            print("\nThe following updates will be applied:\n")
            for update in updates_needed:
                print(Updates.explain(update))
            if confirm:
                confirmation = input(
                    "\nType 'y'to confirm, anything else to cancel: "
                )
                if confirmation.strip() != 'y':
                    print("\n*** Upgrade canceled ***\n")
                    sys.exit(0)

            for update in updates_needed:
                Updates.do_update(update)
        else:
            print("\n*** Brightway2 is up to date! ***\n")


def main():
    args = docopt(__doc__, version='Brightway2 up to date 0.1')
    updater_interface = UpdaterInterface()
    if args['--list'] or args['-l']:
        updater_interface.list()
    else:
        updater_interface.update()


if __name__ == "__main__":
    main()
