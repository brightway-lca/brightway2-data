Import and Export
*****************

BW2Package
==========

Brightway2 has its own data format for efficient saving, loading, and transfer. Read more at the `Brightway2 documentation <http://brightway2.readthedocs.org/en/latest/key-concepts.html#data-interchange>`_.

.. note:: **imports** and **exports** are supported.


.. autoclass:: bw2data.io.BW2PackageImporter
    :members:

.. autoclass:: bw2data.io.BW2PackageExporter
    :members:

Ecospold1
=========

Ecospold version 1 is the data format of ecoinvent versions 1 and 2, and the US LCI. It is an XML data format with reasonable defaults.

.. note:: only **imports** are supported.

.. autoclass:: bw2data.io.EcospoldImporter
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
