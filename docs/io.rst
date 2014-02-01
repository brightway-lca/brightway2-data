Import and Export
*****************

BW2Package
==========

Brightway2 has its own data format for archiving data which is both efficient and compatible across operating systems and programming languages. This is the default backup format for Brightway2 :ref:`datastore` objects.

.. note:: **imports** and **exports** are supported.

.. autoclass:: bw2data.io.BW2Package
    :members:

Ecospold1
=========

Ecospold version 1 is the data format of ecoinvent versions 1 and 2, and the US LCI. It is an XML data format with reasonable defaults.

.. note:: only **imports** are supported.

.. autoclass:: bw2data.io.Ecospold1Importer
    :members:

.. autoclass:: bw2data.io.EcospoldImpactAssessmentImporter
    :members:

SimaPro
=======

Import a `SimaPro <http://www.pre-sustainability.com/simapro-lca-software>`_ text file.

.. note:: only **imports** are supported.

.. autoclass:: bw2data.io.SimaProImporter
    :members:

Gephi
=====

`Gephi <http://gephi.org/>`_ is an open-source graph visualization and analysis program.

.. note:: only **exports** are supported.

.. autoclass:: bw2data.io.DatabaseToGEXF
    :members:

.. autoclass:: bw2data.io.DatabaseSelectionToGEXF
    :members:

.. autofunction:: bw2data.io.keyword_to_gephi_graph
