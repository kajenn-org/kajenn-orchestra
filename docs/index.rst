kajenn-orchestra documentation
==============================

.. raw:: html

   <div class="brand-lockup">
     <img class="brand-light" src="_static/branding/kajenn-orchestra-logo.png" alt="kajenn orchestra" width="190">
     <img class="brand-dark" src="_static/branding/kajenn-orchestra-logo-dark.png" alt="kajenn orchestra" width="190">
   </div>


Orchestration for kajenn: the SPA application, a supervised pool of worker
processes, users pinned to one worker, live mobility by freeze and
reassignment.

**kajenn-orchestra** adds one mountable application to a `kajenn
<https://kajenn.readthedocs.io/en/latest/>`_ server — ``SpaApplication``, the
front — and everything standing behind it: a commander, one or more worker
groups, the child processes that host your site, the freezer on disk that holds
a user while he has no process, and the registers that say who exists and where.
Based on genropy history and genro-modules.

Every page a user opens is served by the same worker process, and his state
lives in that process's memory. He reaches another process through one path: he
is held, his state is written to the freezer, his placement is cleared, and his
next request wakes him wherever the placement lands. Nothing replicates, and the
only thing shared between workers is one dictionary the commander owns.

Installing ``kajenn-orchestra`` installs ``kajenn`` with it. The dependency runs
one way: ``kajenn_orchestra`` imports ``kajenn``, ``kajenn`` never imports
``kajenn_orchestra``, and ``import kajenn`` loads none of this. Read the
``kajenn`` documentation first — the server, the mounted applications, the
configuration recipe and the websocket channel are described there, and these
pages assume them.

These pages describe the development checkout. Source and release packages may
differ; see :doc:`building` for local builds and publication status.

.. toctree::
   :maxdepth: 2
   :caption: Getting started

   getting-started

.. toctree::
   :maxdepth: 2
   :caption: Concepts

   concepts

.. toctree::
   :maxdepth: 2
   :caption: Guides

   guides/index

.. toctree::
   :maxdepth: 2
   :caption: Architecture

   architecture/overview

.. toctree::
   :maxdepth: 2
   :caption: Configuration reference

   configuration

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/index

.. toctree::
   :maxdepth: 1
   :caption: FAQ

   faq

.. toctree::
   :maxdepth: 1
   :caption: Building

   building

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
