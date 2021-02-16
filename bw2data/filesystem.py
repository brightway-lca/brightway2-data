import hashlib
import os
import re

re_slugify = re.compile(r"[^\w\s-]", re.UNICODE)


def create_dir(dirpath):
    "Create directory tree to `dirpath`; ignore if already exists"
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)


def check_dir(directory):
    """Returns ``True`` if given path is a directory and writeable, ``False`` otherwise."""
    return os.path.isdir(directory) and os.access(directory, os.W_OK)


def md5(filepath, blocksize=65536):
    """Generate MD5 hash for file at `filepath`"""
    hasher = hashlib.md5()
    fo = open(filepath, "rb")
    buf = fo.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = fo.read(blocksize)
    return hasher.hexdigest()
