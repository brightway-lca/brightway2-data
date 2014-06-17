Inventory data backends
***********************

.. _database:

DatabaseChooser
===============

The function ``bw2data.database.Database`` is an alias for ``bw2data.database.DatabaseChooser``, which will create an instance of database backend given in the database metadata.

.. autoclass:: bw2data.database.DatabaseChooser
    :members:

Custom database backends
========================

New database backends should inherit from ``bw2data.backends.base.LCIBackend``:

.. autoclass:: bw2data.backends.base.LCIBackend
    :members:

Default backend - each database is a single file
================================================

.. autoclass:: bw2data.backends.default.database.SingleFileDatabase
    :members:

Version-control friendly - each database is a JSON file
=======================================================

.. autoclass:: bw2data.backends.json.database.JSONDatabase
    :members:

.. autoclass:: bw2data.backends.json.sync_json_dict.SynchronousJSONDict
    :members:

.. autoclass:: bw2data.backends.json.sync_json_dict.frozendict
    :members:
