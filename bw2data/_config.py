# -*- coding: utf-8 -*-
import codecs
import json
import os
import platform
import sys
import tempfile
import warnings


class Config(object):
    """A singleton that store configuration settings. Default data directory is ``brightway2`` in the user's home directory, and is stored as ``config.dir``. Other configuration settings can also be assigned as needed.

    Args:
        * *path* (str, optional): The path of the data directory. Must be writeable.

    """
    version = 2
    basic_directories = ("processed", "intermediate", "backups", "logs")
    backends = {}
    cache = {}
    _windows = platform.system() == "Windows"

    def __init__(self, path=None):
        self.reset(path)

    def check_dir(self, directory=None):
        """Returns ``True`` if given path is a directory and writeable, ``False`` otherwise. Default ``directory`` is the Brightway2 data directory."""
        return os.path.isdir(self.dir) and \
            os.access(directory or self.dir, os.W_OK)

    def reset(self, path=None):
        """Reset to original configuration. Useful for testing."""
        self.dir = self.decode_directory(self.get_home_directory(path))
        if not self.check_dir():
            warnings.warn(u"Data directory is: {}\nERROR: This directory "
                          u"doesn't exist or is not writable")
            sys.exit(1)
        self.create_basic_directories()
        self.load_preferences()

    def load_preferences(self):
        """Load a set of preferences from a file in the data directory.

        Preferences as stored as ``config.p``."""
        try:
            self.p = json.load(open(os.path.join(
                self.dir, "preferences.json")))
        except:
            # Create new file
            self.p = {}
            self.save_preferences()

    def save_preferences(self):
        """Serialize preferences to disk."""
        with open(os.path.join(
                self.dir,
                "preferences.json"), "w") as f:
            json.dump(self.p, f, indent=2)

    @property
    def biosphere(self):
        """Get name for ``biosphere`` database from user preferences.

        Default name is ``biosphere``; change this by changing ``config.p["biosphere_database"]``. Don't forget ``config.save_preferences()`` to save changes."""
        if not hasattr(self, "p"):
            self.load_preferences()
        return self.p.get(u"biosphere_database", u"biosphere3")

    @property
    def global_location(self):
        """Get name for global location from user preferences.

        Default name is ``GLO``; change this by changing ``config.p["global_location"]``. Don't forget ``config.save_preferences()`` to save changes."""
        if not hasattr(self, "p"):
            self.load_preferences()
        return self.p.get(u"global_location", u"GLO")

    def decode_directory(self, path):
        """Decode ``path`` from unicode code points if necessary."""
        if os.path.supports_unicode_filenames:
            # Leaving as unicode is OK
            return path
        else:
            # posix systems except for Mac OS X
            return path.encode('utf-8')

    def get_home_directory(self, path=None):
        """Get data directory, trying in order:

        * Provided ``path`` (optional)
        * ``BRIGHTWAY2_DIR`` environment variable
        * ``.brightway2path`` file in user's home directory
        * ``brightway2path.txt`` file in user's home directory
        * ``brightway2data`` in user's home directory

        To set the environment variable:

        * Unix/Mac: ``export BRIGHTWAY2_DIR=/path/to/brightway2/directory``
        * Windows XP: Instead of an environment variable, just create a ``brightway2path.txt`` file in your home directory (C:\Users\Your Name\) with a single line that is the directory path you want to use.
        * Windows 7/8: ``setx BRIGHTWAY2_DIR=\path\\to\\brightway2\directory``

        """
        if path:
            self._dir_from = u"user provided"
            return path
        user_dir = os.path.expanduser("~")
        envvar = os.getenv(u"BRIGHTWAY2_DIR")
        if envvar:
            self._dir_from = u"environment variable"
            return envvar
        for filename in (u".brightway2path", u"brightway2path.txt"):
            try:
                candidate = codecs.open(
                    os.path.join(user_dir, filename),
                    encoding='utf-8'
                ).readline().strip()
                self._dir_from = filename
                return candidate
            except:
                pass
        self._dir_from = u"default"
        default_path = os.path.join(user_dir, "brightway2data")
        if not os.path.exists(default_path):
            os.mkdir(default_path)
        return default_path

    def request_dir(self, dirname):
        """Return the absolute path to the subdirectory ``dirname``, creating it if necessary.

        Returns ``False`` if directory can't be created."""
        path = os.path.join(self.dir, dirname)
        if self.check_dir(path):
            return path
        else:
            try:
                os.mkdir(path)
                return path
            except:
                return False

    def create_basic_directories(self):
        """Create basic directory structure.

        Useful when first starting or for tests."""
        for name in self.basic_directories:
            if not os.path.exists(os.path.join(self.dir, name)):
                os.mkdir(os.path.join(self.dir, name))


config = Config()
