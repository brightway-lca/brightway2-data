# -*- coding: utf-8 -*
from .. import Database


class Activity(object):
    """
Simple proxy for an activity dataset. Makes manipulation and use in command line more convenient.

.. warning:: This proxy is read only! To save changes to a dataset, you will need to work with the raw database data.

Instantiate a activity proxy with its key, e.g. ``("foo", "bar")``:

.. code-block:: python

    activity = Activity(("foo", "bar"))

Properties:

* ``code``
* ``database``
* ``exchanges`` (returns a list of :ref:`exchange` objects.)

See also the descriptions of each method below.

    """
    def __init__(self, key):
        self.key = key

    # Magic methods to make Activity have the same behavior in dictionaries
    # as the normal ("foo", "bar") key

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return self.key == other

    def __str__(self):
        return str(self.key)

    def __repr__(self):
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        try:
            return u"'%s' (%s, %s, %s)" % (self.name, self.unit, self.location,
                                           self.categories)
        except:
            return u"Error with key %s" % (self.key, )

    def __getitem__(self, key):
        if key == 0:
            return self.database
        elif key == 1:
            return self.code
        else:
            return self.raw[key]

    def __setitem__(self, key, value):
        raise AttributeError("Activity proxies are read-only.")

    def __contains__(self, key):
        return key in self.raw

    def __getattr__(self, attr):
        attr = unicode(attr)
        if attr in self:
            return self.raw[attr]
        else:
            return None

    @property
    def exchanges(self):
        from .exchange import Exchange
        return [Exchange(exc) for exc in self.raw.get(u'exchanges', [])]

    @property
    def database(self):
        return self.key[0]

    @property
    def code(self):
        return self.key[1]

    def lca(self, method=None, amount=1.):
        """Shortcut to construct an LCA object for this activity."""
        from bw2calc import LCA
        lca = LCA({self: amount}, method=method)
        lca.lci()
        if method is not None:
            lca.lcia()
        lca.fix_dictionaries()
        return lca

    # Methods not normally needed in public API
    @property
    def raw(self):
        if not hasattr(self, "_raw"):
            self._raw = Database(self.key[0]).get(self.key[1])
        return self._raw
