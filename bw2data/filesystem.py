# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

import hashlib
import os
import re
import unicodedata

re_slugify = re.compile('[^\w\s-]', re.UNICODE)


def safe_filename(string, add_hash=True):
    """Convert arbitrary strings to make them safe for filenames. Substitutes strange characters, and uses unicode normalization.

    if `add_hash`, appends hash of `string` to avoid name collisions.

    From http://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename-in-python"""
    safe = re.sub(
        '[-\s]+',
        '-',
        str(
            re_slugify.sub(
                '',
                unicodedata.normalize('NFKD', str(string))
            ).strip()
        )
    )
    if add_hash:
        if isinstance(string, str):
            string = string.encode("utf8")
        return safe + u"." + hashlib.md5(string).hexdigest()
    else:
        return safe


def create_dir(dirpath):
    "Create directory tree to `dirpath`; ignore if already exists"
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)


def check_dir(directory):
    """Returns ``True`` if given path is a directory and writeable, ``False`` otherwise."""
    return os.path.isdir(directory) and os.access(directory, os.W_OK)
