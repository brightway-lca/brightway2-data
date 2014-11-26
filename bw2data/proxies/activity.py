# -*- coding: utf-8 -*
from .. import Database
from ..utils import open_activity_in_webbrowser
import re
import urllib2

url_pattern = re.compile("/view/(?P<database>.+)/(?P<code>.+)$")


class NonURLKey(StandardError):
    """Key is not a recognized bw2-web URL"""
    pass


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
        try:
            self.key = self.decompose_url(key)
        except NonURLKey:
            self.key = key

    def decompose_url(self, key):
        try:
            fa = url_pattern.findall(urllib2.unquote(key))
            assert len(fa) == 1
            return fa[0]
        except:
            raise NonURLKey

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
        from bw2simple.exchange import Exchanges
        return Exchanges(self.raw.get(u'exchanges', []), self)

    @property
    def database(self):
        return self.key[0]

    @property
    def code(self):
        return self.key[1]

    def open_in_webbrowser(self):
        """Open this activity in the web UI.

        Requires `bw2-web` to be running."""
        open_activity_in_webbrowser(self)

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
            self._raw = Database(self.key[0]).load()[self]
        return self._raw
