from serialization import SerializedDict, PickledDict, CompoundJSONDict


class Mapping(PickledDict):
    """A dictionary that maps object ids, like ``("Ecoinvent 2.2", 42)``, to integers. Needed because parameter arrays have integer ``row`` and ``column`` fields.

    File data is saved in ``mapping.pickle``.

    This dictionary does not support setting items directly; instead, use the ``add`` method to add multiple keys."""
    _filename = "mapping.pickle"

    def add(self, keys):
        """Add a set of keys. These keys can already be in the mapping; only new keys will be added.

        Args:
            * *keys* (list): The keys to add.

        """
        index = max(self.data.values()) if self.data else 0
        for i, key in enumerate(keys):
            if key not in self.data:
                self.data[key] = index + i + 1
        self.flush()

    def delete(self, keys):
        """Delete a set of keys.

        Args:
            *keys* (list): The keys to delete.

        """
        for key in keys:
            del self.data[key]
        self.flush()

    def __setitem__(self, key, value):
        raise NotImplemented

    def __unicode__(self):
        return u"Mapping from databases and methods to parameter indices."

    def __len__(self):
        return len(self.data)


class GeoMapping(Mapping):
    """A dictionary that maps location codes to integers. Needed because parameter arrays have integer ``geo`` fields.

    File data is stored in ``geomapping.pickle``.

    This dictionary does not support setting items directly; instead, use the ``add`` method to add multiple keys."""
    _filename = "geomapping.pickle"

    def __init__(self, *args, **kwargs):
        super(GeoMapping, self).__init__(*args, **kwargs)
        # At a minimum, "GLO" should always be present
        self.add(["GLO"])

    def __unicode__(self):
        return u"Mapping from locations to parameter indices."


class Databases(SerializedDict):
    """A dictionary for database metadata. This class includes methods to manage database versions. File data is saved in ``databases.json``."""
    _filename = "databases.json"

    def increment_version(self, database, number=None):
        """Increment the ``database`` version. Returns the new version."""
        self.data[database]["version"] += 1
        if number is not None:
            self.data[database]["number"] = number
        self.flush()
        return self.data[database]["version"]

    def version(self, database):
        """Return the ``database`` version"""
        return self.data[database]["version"]

    def __unicode__(self):
        return u"Brightway2 databases metadata with %i objects" % len(
            self.data)


class Methods(CompoundJSONDict):
    """A dictionary for method metadata. File data is saved in ``methods.json``."""
    _filename = "methods.json"

    def __unicode__(self):
        return u"Brightway2 methods metadata with %i objects" % len(
            self.data)


class WeightingMeta(Methods):
    """A dictionary for weighting metadata. File data is saved in ``methods.json``."""
    _filename = "weightings.json"


class NormalizationMeta(Methods):
    """A dictionary for normalization metadata. File data is saved in ``methods.json``."""
    _filename = "normalizations.json"


mapping = Mapping()
databases = Databases()
methods = Methods()
geomapping = GeoMapping()
weightings = WeightingMeta()
normalizations = NormalizationMeta()


def reset_meta():
    mapping.__init__()
    databases.__init__()
    methods.__init__()
    geomapping.__init__()
    weightings.__init__()
    normalizations.__init__()
