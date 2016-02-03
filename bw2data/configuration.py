# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

import eight
import json
import os
import platform
import sys
import tempfile
import warnings
from .filesystem import check_dir

# os.getenv returns unicode in Py2
eight.wrap_os_environ_io()


class Config(object):
    """A singleton that stores configuration settings"""
    version = 3
    backends = {}
    cache = {}
    metadata = []
    sqlite3_databases = []
    _windows = platform.system() == "Windows"

    @property
    def biosphere(self):
        """Get name for ``biosphere`` database from user preferences.

        Default name is ``biosphere3``; change this by changing ``config.p["biosphere_database"]``."""
        return self.p.get("biosphere_database", "biosphere3")

    @property
    def global_location(self):
        """Get name for global location from user preferences.

        Default name is ``GLO``; change this by changing ``config.p["global_location"]``."""
        return self.p.get("global_location", "GLO")

    ### Compatibility layer for < 2.0

    def reset(self, *args, **kwargs):
        warnings.warn(
            "`reset` is deprecated and doesn't do anything; please change "
            "projects using `projects.current = 'foo'",
            DeprecationWarning
        )

    @property
    def dir(self):
        warnings.warn(
            "`config.dir` is deprecated; please use `projects.dir` instead",
            DeprecationWarning
        )

    def check_dir(self, directory=None):
        warnings.warn("`config.check_dir` is deprecated, please use `filesystem.check_dir`", DeprecationWarning)

    def load_preferences(self):
        warnings.warn(
            "`config.load_preferences` is deprecated; preferences are loaded automatically",
            DeprecationWarning
        )

    def save_preferences(self):
        """Serialize preferences to disk."""
        warnings.warn(
            "`config.save_preferences` is deprecated; preferences are saved automatically",
            DeprecationWarning
        )

    def request_dir(self, dirname):
        warnings.warn(
            "`config.request_dir` is deprecated; please use `projects.request_directory`",
            DeprecationWarning
        )

    def get_home_directory(self, path=None):
        """Get data directory, trying in order:

        * Provided ``path`` (optional)
        * ``BRIGHTWAY2_DIR`` environment variable
        * ``.brightway2path`` file in user's home directory
        * ``brightway2path.txt`` file in user's home directory
        * ``brightway2data`` in user's home directory

        To set the environment variable:

        * Unix/Mac: ``export BRIGHTWAY2_DIR=/path/to/brightway2/directory``
        * Windows XP: Instead of an environment variable, just create a ``brightway2path.txt`` file in your home directory (C:\\Users\\Your Name\\) with a single line that is the directory path you want to use.
        * Windows 7/8: ``setx BRIGHTWAY2_DIR=\\\\path\\\\to\\\\brightway2\\\\directory``

        """
        warnings.warn(
            "`config.get_home_directory` is deprecated; the data directory is now managed automatically. Start a new project with `projects.current = 'my new project'`",
            DeprecationWarning
        )
        user_dir = os.path.expanduser("~")
        envvar = os.getenv("BRIGHTWAY2_DIR")
        if envvar:
            self._dir_from = "environment variable"
            if not self.check_dir(envvar):
                warnings.warn("The environment variable BRIGHTWAY2_DIR was set, "
                    "but doesn't exist or is not writable."
                )
            return envvar
        for filename in (".brightway2path", "brightway2path.txt"):
            try:
                candidate = open(
                    os.path.join(user_dir, filename),
                    encoding='utf-8'
                ).readline().strip()
                self._dir_from = filename
                return candidate
            except:
                pass
        self._dir_from = "default"
        default_path = os.path.join(user_dir, "brightway2data")
        if not os.path.exists(default_path):
            os.mkdir(default_path)
        return default_path

    def create_basic_directories(self):
        warnings.warn("`config.create_basic_directories` is deprecated; "
            "Basic directories are created automatically when starting a new project",
            DeprecationWarning)

config = Config()
