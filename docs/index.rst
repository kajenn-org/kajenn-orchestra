kajenn-orchestra documentation
==============================

.. raw:: html

   <div class="brand-lockup">
     <img class="brand-light" src="_static/branding/kajenn-orchestra-logo.png" alt="kajenn orchestra" width="190">
     <img class="brand-dark" src="_static/branding/kajenn-orchestra-logo-dark.png" alt="kajenn orchestra" width="190">
   </div>


**kajenn-orchestra** puts a pool of worker processes behind a `kajenn
<https://kajenn.readthedocs.io/en/latest/>`_ server: the SPA front, the commander, the worker
processes, the registers and the hosted ASGI/WSGI adapters. It depends on
``kajenn`` and adds the Python package ``kajenn_orchestra``; importing the core
never loads it. Based on genropy history and genro-modules.

Read the ``kajenn`` documentation first: the server, the routed applications,
the configuration recipe and the WSX transport are all described there. These
pages cover only what orchestration adds.

These pages describe the development checkout. Source and release packages may
differ; see :doc:`building` for local builds and publication status.

.. toctree::
   :maxdepth: 2
   :caption: Getting started

   building

.. toctree::
   :maxdepth: 2
   :caption: Guides

   guides/index

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/index

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
