from . import config
from contextlib import contextmanager


class _Fore(object):
    def __getattr__(self, *args, **kwargs):
        return ""

if not hasattr(config, "p"):
    config.load_preferences()

if not config._ipython and not config.p.get('no_color'):
    from colorama import Fore, init, deinit

    # ASCII color codes
    # class Fore(object):
    #     BLACK = '\x1b[30m'
    #     CYAN = '\x1b[36m'
    #     MAGENTA = '\x1b[35m'
    #     RESET = '\x1b[39m'
    #     YELLOW = '\x1b[33m'
    #     BLUE = '\x1b[34m'
    #     GREEN = '\x1b[32m'
    #     RED = '\x1b[31m'
    #     WHITE = '\x1b[37m'
else:
    # IPython seems to screw up colorama, maybe by not printing to stdout
    # Can globally disable for windows as well...
    # See also http://stackoverflow.com/questions/9848889/colorama-for-python-not-returning-colored-print-lines-on-windows
    Fore = _Fore()

    def init(*args, **kwargs):
        return

    def deinit(*args, **kwargs):
        return

@contextmanager
def safe_colorama():
    init()
    try:
        yield
    finally:
        deinit()
