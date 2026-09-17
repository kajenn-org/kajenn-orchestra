Multiworker SPA integration
===========================

These are integration surfaces of the separate ``kajenn_orchestra``
package. See :doc:`../guides/multiworker-spa` before configuring a pool.

Front and worker
----------------

.. autoclass:: kajenn_orchestra.spa_app.SpaApplication
   :members: commander, handshake_cookie

.. autoclass:: kajenn_orchestra.orchestration.spa_worker.SpaWorker
   :members: hosted_app_seam, global_store, send_message, run_sync, build_request_slot, on_request_served

Hosted application adapters
---------------------------

.. automodule:: kajenn_orchestra.environ
   :members: AsgiSeam, WsgiSeam

Global store
------------

.. autoclass:: kajenn_orchestra.global_store.GlobalStoreClient
   :members: get, set, delete, for_update

.. autoclass:: kajenn_orchestra.global_store.GlobalStoreLease
   :members: abort

.. autoexception:: kajenn_orchestra.global_store.GlobalStoreCommitUnconfirmed
