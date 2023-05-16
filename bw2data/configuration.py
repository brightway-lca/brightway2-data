# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals

import platform

import eight

# os.getenv returns unicode in Py2
eight.wrap_os_environ_io()

try:
    from bw2parameters import config as bw2parameters_config
except ImportError:
    raise ImportError(
        "Installed version of bw2parameters is outdated. Please install version > 1.0.0."
    )


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

        Default name is ``biosphere3``; change this by changing ``config.p["biosphere_database"]``.
        """
        return self.p.get("biosphere_database", "biosphere3")

    @property
    def global_location(self):
        """Get name for global location from user preferences.

        Default name is ``GLO``; change this by changing ``config.p["global_location"]``.
        """
        return self.p.get("global_location", "GLO")

    @property
    def use_pint_parameters(self):
        return bw2parameters_config.use_pint

    @use_pint_parameters.setter
    def use_pint_parameters(self, value):
        bw2parameters_config.use_pint = value


config = Config()
