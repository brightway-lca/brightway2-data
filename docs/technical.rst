Technical guide
***************

Modular structure
=================

Brightway2 is a framework for life cycle assessment, and consists of several packages. You only need to install or understand the components that are of interest to you. Splitting components allow for a clean separation of concerns, as each package has a limited focus, and makes testing and documenting each package easier and cleaner.

This guide has technical details for the ``bw2data`` package. Each separate package also has its own documentation.

The current components of Brightway2 are:

* bw2data: This package provides data handling, querying, and import/export functionality.
* bw2calc: The LCA calculators. Normal LCA, several varieties of Monte Carlo LCA (including parallel Monte Carlo using all the cores on your computer), Latin Hypercubic sampling, and graph traversal.
* bw2analyzer: Functions for analyzing the results of LCA calculations, including contribution and sensitivity analysis.
* bw2ui: Two different user interfaces for Brightway2. Brightway2 is pure Python, and can be used by other programs or in an ipython notebook. For people who aren't as comfortable programming in Python, this packages provides a command line interface for work in the terminal, and a web interface.

Reference
=========

.. toctree::
   :maxdepth: 2

   configuration
   metadata
   database
   method
   io
