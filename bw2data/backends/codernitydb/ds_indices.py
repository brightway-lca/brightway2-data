from CodernityDB.hash_index import HashIndex, UniqueHashIndex
from CodernityDB.tree_index import TreeBasedIndex
from ...units import normalize_units
from . import uniencode


class KeyIndex(UniqueHashIndex):
    custom_header = """from bw2data.backends.codernitydb import uniencode"""

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(KeyIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        """Return key, value. Use for storing in index."""
        if "database" not in data or "code" not in data:
            return None
        return uniencode(data['database'] + data['code']), None

    def make_key(self, data):
        """Process value used in database query to correct key value."""
        return uniencode("".join(data))


class DatabaseNameIndex(TreeBasedIndex):
    custom_header = """from bw2data.backends.codernitydb import uniencode
from CodernityDB.tree_index import TreeBasedIndex"""

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(DatabaseNameIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        return uniencode(data['database']), None

    def make_key(self, key):
        return uniencode(key)


class CodeIndex(HashIndex):
    custom_header = """from bw2data.backends.codernitydb import uniencode"""

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(CodeIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        return uniencode(data['code']), None

    def make_key(self, code):
        return uniencode(code)


class LocationIndex(TreeBasedIndex):
    custom_header = """from bw2data.backends.codernitydb import uniencode
from CodernityDB.tree_index import TreeBasedIndex"""

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(LocationIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        if u"location" not in data:
            return None
        return uniencode(data[u'location']), None

    def make_key(self, location):
        return uniencode(location)


class ReferenceProductIndex(TreeBasedIndex):
    custom_header = """from bw2data.backends.codernitydb import uniencode
from CodernityDB.tree_index import TreeBasedIndex"""

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(ReferenceProductIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        if u"reference product" not in data:
            return None
        return uniencode(data[u'reference product']), None

    def make_key(self, rprod):
        return uniencode(rprod)


class UnitIndex(TreeBasedIndex):
    custom_header = """from bw2data.backends.codernitydb import uniencode
from CodernityDB.tree_index import TreeBasedIndex
from bw2data.units import normalize_units"""

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(UnitIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        if u"unit" not in data:
            return None
        return uniencode(normalize_units(data[u"unit"])), None

    def make_key(self, unit):
        return uniencode(normalize_units(unit))
