# The MIT License (MIT)

# Copyright (c) 2014 abarnert

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# fatomic from abarnert
# https://github.com/abarnert/fatomic
import contextlib
import errno
import os
import shutil
import sys
import tempfile
import types

__all__ = [
    "replace",
    "open",
    "writeall",
    "writechunks",
    "writelines",
    "transformall",
    "transformchunks",
    "transformlines",
    "transform",
]

# If we're on 3.3+, just use os.replace; if we're on POSIX, rename
# and replace do the same thing.
try:
    replace = os.replace
except AttributeError:
    if sys.platform != "win32":
        replace = os.rename
    else:
        # This requires PyWin32 if you're on Windows. If that's not
        # accepted, you can write a ctypes solution, but then you'll
        # have to handle unicode-vs.-bytes strings and creating an
        # OSError from GetLastError and so on yourself, which I don't
        # feel like doing. (I'll accept a pull request from anyone
        # else who does...)
        import win32api
        import win32con

        def replace(src, dst):
            win32api.MoveFileEx(src, dst, win32con.MOVEFILE_REPLACE_EXISTING)


def _guessmode(contents, binary):
    if binary is not None:
        return binary
    if isinstance(contents, str):
        return False
    elif isinstance(contents, (bytes, bytearray)):
        return True
    else:
        return False


def _mode(binary, contents=None):
    if binary is not None:
        return "b" if binary else ""
    if contents is None:
        raise TypeError("binary cannot be None")
    if isinstance(contents, str):
        return ""
    elif isinstance(contents, (bytes, bytearray)):
        return "b"
    else:
        return ""


def _tempfile(filename, mode):
    return tempfile.NamedTemporaryFile(
        mode=mode, prefix=os.path.basename(filename), suffix=".tmp", delete=False
    )


_open = open


@contextlib.contextmanager
def open(filename, mode, *args, **kwargs):
    if mode[0] not in "wxa" or len(mode) > 1 and mode[1] == "+":
        raise ValueError("invalid mode: '{}'".format(mode))
    f = _tempfile(filename, mode, *args, **kwargs)
    _discard = [False]
    try:
        if mode[0] == "a":
            try:
                with _open(filename, "r" + mode[1:], *args, **kwargs) as fin:
                    shutil.copyfileobj(fin, f)
            except (OSError, IOError) as e:
                if e.errno == errno.ENOENT:
                    pass

        def discard(self, _discard=_discard):
            _discard[0] = True

        f.discard = types.MethodType(discard, f)
        yield f
    finally:
        f.close()
        if not _discard[0]:
            try:
                replace(f.name, filename)
            except OSError:
                shutil.move(f.name, filename)


def write(filename, lines, binary=False):
    mode = _mode(binary)
    with open(filename, "w" + mode) as fout:
        fout.writelines(lines)


def writeall(filename, contents, binary=None):
    mode = _mode(binary, contents)
    with open(filename, "w" + mode) as fout:
        f.write(contents)


writechunks = write


def transform(filename, func, binary=False):
    mode = _mode(binary)
    with open(filename, "w" + mode) as fout, _open(filename, "r" + mode) as fin:
        fout.writelines(func(line) for line in fin)


def transformall(filename, func, binary=False):
    mode = _mode(binary)
    with open(filename, "w" + mode) as fout, _open(filename, "r" + mode) as fin:
        fout.writelines(fin.read())


def _chunkfile(f, chunksize=None):
    if chunksize is None:
        chunksize = 8192
    while True:
        buf = f.read(chunksize)
        if not buf:
            return
        yield buf


def transformchunks(filename, func, chunksize=None, binary=False):
    mode = _mode(binary)
    with open(filename, "w" + mode) as fout, _open(filename, "r" + mode) as fin:
        fout.writelines(_chunkfile(fin, chunksize))


def append(filename, lines, binary=False):
    mode = _mode(binary)
    with open(filename, "a" + mode) as fout:
        fout.writelines(lines)


def appendall(filename, contents, binary=False):
    mode = _mode(binary)
    with open(filename, "a" + mode) as fout:
        fout.write(contents)


appendchunks = append


def test():
    with open("foo", "a") as f:
        f.write("Hello, ")
    with open("foo", "a") as f:
        f.write("world\n")
    transform("foo", lambda line: line.replace("Hello", "Hi"))


if __name__ == "__main__":
    test()
