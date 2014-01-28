Brightway2-data
===============

This is the documentation for Brightway2-data, part of the `Brightway2 <http://brightwaylca.org>`_ life cycle assessment framework.

Surprisingly enough, Brightway2-data (abbreviated to bw2data in code) is the package the manages different types of data in Brightway2. In general, Brightway2-data can save, load, process, validate, import and export different kinds of data. It also includes code to setup the data directory, query datasets, and normalize units.

This page of the documentation covers the basic concepts in Brightway2-data. Documentation on querying, and import and export of data in different formats, are in separate sections.

.. toctree::
   :maxdepth: 1

   querying
   io

Other resources
---------------

The following online resources are available:

* `Source code <https://bitbucket.org/cmutel/brightway2-data>`_
* `Documentation on Read the Docs <http://bw2data.readthedocs.org>`_
* `Test coverage report <http://coverage.brightwaylca.org/data/index.html>`_

Configuration
=============

The first thing Brightway2 needs is to know where it can save data and log files. This directory location, in addition to a number of other configuration variables, is managed by the :ref:`configuration` object.

The ``config`` object stores the Brightway2 directory, and can also change it, and create new directories. It also stores information about whether or not it is being run on Windows, or used in an iPython shell.

The ``config`` object also stores user preferences. User preferences include things like the default number of Monte Carlo iterations to run, but it is just a dictionary, and can be added to as desired.

.. warning:: Preferences are not saved automatically - you must call ``config.save_preferences()``.

Data and metadata
=================

.. note:: For more detailed information, see `tutorial 5: defining a new matrix <http://nbviewer.ipython.org/url/brightwaylca.org/tutorials/Tutorial%205%20-%20Defining%20A%20New%20Matrix.ipynb>`_.

The building blocks in Brightway2 data are the **data store** and the **metadata store**. The difference between the two can be easily explained in the example of LCI databases:

    * The data store object, :ref:`databases`, has the actual activity data for each database.
    * The metadata store, :ref:`database`, has information about the database, like the format it is in, its version number, and what other databases it links to.

Both the data and metadata objects *store* data, and provide easy ways to save and load data.

Metadata stores
---------------

The base class for metadata is :ref:`serialized-dict`, which is basically a normal dictionary that can be easily saved or loaded (i.e. serialized) to or from a `JSON <http://en.wikipedia.org/wiki/JSON>`_ file. These files can be easily edited in a normal text editor.

Brightway2-data defines the following metadata stores:

    * :ref:`databases`: LCI databases
    * :ref:`methods`: LCIA methods (characterization factors)
    * :ref:`normalizations`: LCIA normalization factors
    * :ref:`weightings`: LCIA weighting factors

There are no required fields of metadata for any metadata stores, though some fields may be added automatically by subclasses.

Metadata stores are just dictionaries that can be easily serialized - they are not associated with a specific data store, and it is possible to use metadata stores without a data store, or with multiple data stores.

Metadata should be singletons
-----------------------------

Metadata stores follow the `singleton pattern <http://en.wikipedia.org/wiki/Singleton_pattern>`_, though this is not enforced. Each metadata dictionary should only exist once, to avoid having multiple conflicting versions. The normal pattern is to instantiate each class in the same file as the class pattern:

.. code-block:: python

    class MyObjects(bw2data.serialization.SerializedDict):
        file = "sweet-peppers.json"

    myobjects = MyObjects()

Data stores
-----------

.. note:: See also `tutorial 2: working with data <http://nbviewer.ipython.org/url/brightwaylca.org/tutorials/Tutorial%202%20-%20Working%20with%20data.ipynb>`_ and `tutorial 5: defining a new matrix <http://nbviewer.ipython.org/url/brightwaylca.org/tutorials/Tutorial%205%20-%20Defining%20A%20New%20Matrix.ipynb>`_.

The base class for data stores is :ref:`datastore`. Each data store subclass defines a schema for its data. The normal methods provided by a data store are:

    * **write(data)**: Write data to disk
    * **load**: Load data from disk
    * **register**: Register object with metadata store
    * **deregister**: Remove object from metadata store
    * **copy(name)**: Create a new object with name ``name``
    * **backup**: Write backup of data
    * **validate(data)**: Validate data using this object's validator

Data store objects are instantiated with the object name, e.g. ``DataStore("name goes here")``.

Brightway2-data defines the following data stores:

    * :ref:`database`
    * :ref:`method`
    * :ref:`weighting`
    * :ref:`normalization`

Validation
----------

Data validation is done using the great `voluptuous library <https://pypi.python.org/pypi/voluptuous/>`_. Each data store can define its own validation schema. See the individual data stores documentation for details on its data format.

Document and processed data
===========================

The basic form of Brightway2 data is *semi-structured* - there are some requirements, and some conventions, but a lot of flexibility. This type of database, is often called a `document database`. However, to construct matrices efficiently from these data documents, a *processing* step is required.

Processing data
---------------

*Processing data* converts document data to a binary form tailored for creating matrices (a NumPy array). All extraneous information is removed, and only the numeric values needed are retained. Put another way, *processing* transforms unstructured data documents to a highly-structured binary form for calculations.

Uncertainty distributions
-------------------------

Uncertainty distributions are modeled using *parameter arrays* from `stats_arrays <https://bitbucket.org/cmutel/stats_arrays>`_, which has its own `extensive documentation <http://stats-arrays.readthedocs.org/en/latest/>`_.

The idea of parameter arrays is to have a common format for defining different uncertainty distributions. Parameter arrays are stored as NumPy `structured or record arrays <http://docs.scipy.org/doc/numpy/reference/generated/numpy.recarray.html#numpy.recarray>`_. The fields that define an uncertainty distribution are:

    * uncertainty type
    * loc (short for location)
    * scale
    * shape
    * minimum
    * maximum
    * negative

In document data, these fields are stored in an *uncertainty dictionary*, e.g.:

.. code-block:: python

    {
        'uncertainty type': NormalUncertainty.id,
        'loc': 0.5,
        'scale': 0.2,
        'minimum': 0  # Acts as bounds; prevent negative values
    }

Default values will be provided if not directly specified.

.. note:: If there is no uncertainty, then a simple number can also be provided. It will be converted automatically to an uncertainty dictionary.

During processing, the uncertainty dictionaries are converted to rows in a NumPy array.

Mappings
========

Sometimes, important data can't be stored as a numeric value. For example, the location of an inventory activity is important for regionalization, but is given by a text string, not an integer. In this case, we use :ref:`serialized-dict` to store mappings between objects are integer indices. Brightway2-data uses two such mappings:

    * :ref:`mapping`: Maps inventory objects (activities, biosphere flows, and anything else that would appear in a supply chain graph) to indices
    * :ref:`geomapping`: Map locations (both inventory and regionalized impact assessment) to indices

Mappings are also singletons. Items are added using ``.add(keys)``, and removed using ``.delete(keys)``.

Development
===========

.. note:: See also the Brightway2 `documentation on contributing <http://brightway2.readthedocs.org/en/latest/contributing.html>`_.

Running tests
-------------

To run the tests, install `nose <https://nose.readthedocs.org/en/latest/>`_, and run ``nosetests``.

Building the documentation
--------------------------

Install `sphinx <http://sphinx.pocoo.org/>`_, and then change to the ``docs`` directory, and run ``make html`` (or ``make.bat html`` in Windows).

Table of Contents
-----------------

.. toctree::
   :maxdepth: 4

   querying
   technical

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
