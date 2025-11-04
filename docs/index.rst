.. dav-tools documentation master file, created by
   sphinx-quickstart on Sun Jul 16 15:00:51 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to sql_error_categorizer's documentation!
=================================================
This project analyses SQL statements to highlight possible **errors**.
The detection engine tokenises the input query and applies a set of rules.
Additional rules are applied on the AST (Abstract Syntax Tree) generated from
the query.
When a rule matches, it reports the type of
error together with the relevant context.

The logic is implemented in `sql_query_analyzer/detectors/` and the available
error identifiers are listed in `sql_query_analyzer/sql_errors.py`.

Below you will find a short explanation that anyone can follow, followed by a
section with technical details for developers.

Contents
========

.. toctree::
   :maxdepth: 4


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Installation
============
``$ pip install sql_error_categorizer``

