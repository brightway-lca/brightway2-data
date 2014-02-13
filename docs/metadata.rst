Metadata
********

Base classes for metadata
=========================

.. _serialized-dict:

Serialized Dictionary
---------------------

.. autoclass:: bw2data.serialization.SerializedDict
    :members:

.. _compound-json:

Compound JSON dictionary
------------------------

JSON hash tables don't support keys like ``("biosphere", "an emission")``, so the ``pack`` and ``unpack`` methods are used to transform data from Python to JSON and back.

.. autoclass:: bw2data.serialization.CompoundJSONDict
    :members:
    :inherited-members:

.. _pickled-dict:

Pickled Dictionary
------------------

.. autoclass:: bw2data.serialization.PickledDict
    :members:
    :inherited-members:

Metadata stores
===============

.. _databases:

databases
---------

.. autoclass:: bw2data.meta.Databases
    :members:
    :inherited-members:

.. _methods:

methods
-------

.. autoclass:: bw2data.meta.Methods
    :members:
    :inherited-members:

.. _normalizations:

normalizations
--------------

.. autoclass:: bw2data.meta.NormalizationMeta
    :members:
    :inherited-members:

.. _weightings:

weightings
----------

.. autoclass:: bw2data.meta.WeightingMeta
    :members:
    :inherited-members:

Mappings
========

.. _mapping:

mapping
-------

.. autoclass:: bw2data.meta.Mapping
    :members:
    :inherited-members:

.. _geomapping:

geomapping
----------

.. autoclass:: bw2data.meta.GeoMapping
    :members:
    :inherited-members:
