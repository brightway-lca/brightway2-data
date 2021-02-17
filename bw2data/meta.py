from .project import writable_project
from .serialization import SerializedDict, PickledDict, CompoundJSONDict
import datetime


class GeoMapping(PickledDict):
    """A dictionary that maps location codes to integers. Needed because parameter arrays have integer ``geo`` fields.

    File data is stored in ``geomapping.pickle``.

    This dictionary does not support setting items directly; instead, use the ``add`` method to add multiple keys."""

    filename = "geomapping.pickle"

    def __init__(self, *args, **kwargs):
        super(GeoMapping, self).__init__(*args, **kwargs)
        # At a minimum, "GLO" should always be present
        if "GLO" not in self:
            self.add(["GLO"])

    @writable_project
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
        raise NotImplementedError

    def __str__(self):
        return "Mapping from databases and methods to parameter indices."

    def __len__(self):
        return len(self.data)


class Databases(SerializedDict):
    """A dictionary for database metadata. This class includes methods to manage database versions. File data is saved in ``databases.json``."""

    filename = "databases.json"

    @writable_project
    def increment_version(self, database, number=None):
        """Increment the ``database`` version. Returns the new version."""
        self.data[database]["version"] += 1
        if number is not None:
            self.data[database]["number"] = number
        self.flush()
        return self.data[database]["version"]

    def version(self, database):
        """Return the ``database`` version"""
        return self.data[database].get("version")

    @writable_project
    def set_modified(self, database):
        self[database]["modified"] = datetime.datetime.now().isoformat()
        self.flush()

    @writable_project
    def set_dirty(self, database):
        self.set_modified(database)
        if self[database].get("dirty"):
            pass
        else:
            self[database]["dirty"] = True
            self.flush()

    def clean(self):
        from . import Database

        @writable_project
        def _clean():
            for x in self:
                if self[x].get("dirty"):
                    Database(x).process()
                    del self[x]["dirty"]
            self.flush()

        if not any(self[x].get("dirty") for x in self):
            return
        else:
            return _clean()

    @writable_project
    def __delitem__(self, name):
        from . import Database

        try:
            Database(name).delete(warn=False)
        except:
            pass

        super(Databases, self).__delitem__(name)


class CalculationSetups(PickledDict):
    """A dictionary for calculation setups.

    Keys:
    * `inv`: List of functional units, e.g. ``[{(key): amount}, {(key): amount}]``
    * `ia`: List of LCIA methods, e.g. ``[(method), (method)]``.

    """

    filename = "setups.pickle"


class DynamicCalculationSetups(PickledDict):
    """A dictionary for Dynamic calculation setups.

    Keys:
    * `inv`: List of functional units, e.g. ``[{(key): amount}, {(key): amount}]``
    * `ia`: Dictionary of orst case LCIA method and the relative dynamic LCIA method, e.g. `` [{dLCIA_method_1_worstcase:dLCIA_method_1 , dLCIA_method_2_worstcase:dLCIA_method_2}]``.

    """

    filename = "dynamicsetups.pickle"


class Methods(CompoundJSONDict):
    """A dictionary for method metadata. File data is saved in ``methods.json``."""

    filename = "methods.json"


class WeightingMeta(Methods):
    """A dictionary for weighting metadata. File data is saved in ``methods.json``."""

    filename = "weightings.json"


class NormalizationMeta(Methods):
    """A dictionary for normalization metadata. File data is saved in ``methods.json``."""

    filename = "normalizations.json"


class Preferences(PickledDict):
    """A dictionary of project-specific preferences."""

    filename = "preferences.pickle"

    def __init__(self, *args, **kwargs):
        super(Preferences, self).__init__(*args, **kwargs)

        # Default preferences
        if "use_cache" not in self:
            self["use_cache"] = True


databases = Databases()
geomapping = GeoMapping()
methods = Methods()
normalizations = NormalizationMeta()
preferences = Preferences()
weightings = WeightingMeta()
calculation_setups = CalculationSetups()
dynamic_calculation_setups = DynamicCalculationSetups()
