# -*- coding: utf-8 -*-
import os
import json
import tempfile
import warnings


class Config(object):
    """A singleton that store configuration settings. Default data directory is ``brightway`` in the user's home directory, and is stored as ``config.dir``. Other configuration settings can also be assigned as needed.

    Args:
        * *path* (str, optional): The path of the data directory. Must be writeable.

    """
    version = 0.1
    basic_directories = ("processed", "intermediate", "backups", "logs")

    # TODO: Create directory if needed (and basic dirs)

    def __init__(self, path=None):
        self.is_temp_dir = False
        self.reset(path)
        self.cache = {}

    def check_dir(self, dir=None):
        """Check is directory is a directory and writeable."""
        return os.path.isdir(self.dir) and os.access(dir or self.dir, os.W_OK)

    def reset(self, path=None):
        """Reset to original configuration. Useful for testing."""
        try:
            self.dir = self.get_home_directory(path)
        except OSError:
            self.dir = tempfile.mkdtemp()
            self.is_temp_dir = True
            if not getattr(self, "dont_warn", False):
                warnings.warn("\n\tYour changes will not be saved!\n"
                    "\tSet a writeable directory!\n"
                    "\tCurrent data directory is:\n"
                    "\t%s" % self.dir, UserWarning)
        self.load_preferences()

    def load_preferences(self):
        """Load a set of preferences from a file in the home directory.

        Preferences as stored as ``config.p``."""
        try:
            self.p = json.load(open(os.path.join(
                self.dir, "preferences.json")))
        except:
            self.p = {"use_cache": True}

    def save_preferences(self):
        """Serialize preferences to disk."""
        with open(os.path.join(self.dir, "preferences.json"), "w") as f:
            json.dump(self.p, f, indent=2)

    def get_home_directory(self, path=None):
        """Get data directory, trying in order:

        * Provided path (optional)
        * ``BRIGHTWAY2_DIR`` environment variable
        * ``.brightway2path`` file in user's home directory
        * ``brightway2path.txt`` file in user's home directory
        * ``brightway2`` in user's home directory

        To set the environment variable:

        * Unix/Mac: ``export BRIGHTWAY2_DIR=/path/to/brightway2/directory``
        * Windows XP: Create a ``brightway2path.txt`` file in your home directory (C:\Users\Your Name\) with a single line that is the directory path you want to use.
        * Windows 7: ``setx BRIGHTWAY2_DIR=\path\to\brightway2\directory``

        """
        if path:
            self._dir_from = "user provided"
            return path
        user_dir = os.path.expanduser("~")
        envvar = os.getenv("BRIGHTWAY2_DIR")
        if envvar:
            self._dir_from = "environment variable"
            return envvar
        for filename in (".brightway2path", "brightway2path.txt"):
            try:
                candidate = open(os.path.join(user_dir, filename)).readline().strip()
                assert os.path.exists(candidate)
                self._dir_from = filename
                return candidate
            except:
                pass
        else:
            self._dir_from = "default"
            return os.path.join(user_dir, "brightway2")

    def request_dir(self, dirname):
        """Return ``True`` if directory already exists or can be created."""
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

    def _get_dir(self):
        return self._dir

    def _set_dir(self, d):
        self._dir = d
        if not self.check_dir():
            raise OSError("This directory is not writeable")

    dir = property(_get_dir, _set_dir)


config = Config()
