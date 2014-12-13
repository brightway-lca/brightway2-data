from . import uniencode
from CodernityDB.hash_index import HashIndex
from CodernityDB.tree_index import TreeBasedIndex


class InputKeyIndex(TreeBasedIndex):
    custom_header = """from bw2data.backends.codernitydb import uniencode
from CodernityDB.tree_index import TreeBasedIndex"""

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(InputKeyIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, exc):
        return uniencode("".join(exc['input'])), None

    def make_key(self, exc):
        """Process value used in database query to correct key value."""
        return uniencode("".join(exc['input']))


class OutputKeyIndex(TreeBasedIndex):
    custom_header = """from bw2data.backends.codernitydb import uniencode
from CodernityDB.tree_index import TreeBasedIndex"""

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(OutputKeyIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, exc):
        return uniencode("".join(exc['output'])), None

    def make_key(self, exc):
        """Process value used in database query to correct key value."""
        return uniencode("".join(exc['output']))


class InputDatabaseIndex(TreeBasedIndex):
    custom_header = """from bw2data.backends.codernitydb import uniencode
from CodernityDB.tree_index import TreeBasedIndex"""

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(InputDatabaseIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, exc):
        return uniencode(exc['input'][0]), None

    def make_key(self, exc):
        """Process value used in database query to correct key value."""
        return uniencode(exc['input'][0])


class OutputDatabaseIndex(TreeBasedIndex):
    custom_header = """from bw2data.backends.codernitydb import uniencode
from CodernityDB.tree_index import TreeBasedIndex"""

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(OutputDatabaseIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, exc):
        return uniencode(exc['output'][0]), None

    def make_key(self, exc):
        """Process value used in database query to correct key value."""
        return uniencode(exc['output'][0])


class ExchangeTypeIndex(TreeBasedIndex):
    custom_header = """from bw2data.backends.codernitydb import uniencode
from CodernityDB.tree_index import TreeBasedIndex"""

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(ExchangeTypeIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, exc):
        return uniencode(exc['type']), None

    def make_key(self, exc):
        """Process value used in database query to correct key value."""
        return uniencode(exc['type'])
