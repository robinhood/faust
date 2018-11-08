.. _changelog:

==============================
 Change history for Faust 1.3
==============================

This document contain change notes for bugfix releases in
the Faust 1.3 series. If you're looking for previous releases,
please visit the :ref:`history` section.

.. _version-1.3.0:

1.3.0
=====
:release-date: 2018-11-08 3:44 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

    + Now depends on :ref:`Mode 2.0.3 <mode:version-2.0.3>`.

    + Now depends on :mod:`robinhood-aiokafka` 1.4.19

- **App**: Refactored rebalancing and table recovery (Issue #185).

    This optimizes the rebalancing callbacks for greater stability.

    Table recovery was completely rewritten to do as little as possible
    during actual rebalance.  This should increase stability and reduce
    the chance of rebalancing loops.

    We no longer attempt to cancel recovery during rebalance,
    so this should also fix problems with hanging during recovery.

- **App**: Adds new :setting:`stream_recovery_delay` setting.

    In this version we are experimenting with sleeping for 10.0 seconds
    after rebalance, to allow for more nodes to join/leave before resuming
    the streams.

    This adds some startup delay, but is in general unnoticeable in
    production.

- **Windowing**: Fixed several edge cases in windowed tables.

    Fix contributed by Omar Rayward (:github_user:`omarrayward`).

- **App**: Skip table recovery on rebalance when no tables defined.

- **RocksDB**: Iterating over table keys/items/values now skips
  standby partitions.

- **RocksDB**: Fixed issue with having "." in table names (Issue #184).

- **App**: Adds :signal:`App.on_rebalance_complete` signal.

- **App**: Adds :signal:`App.on_before_shutdown` signal.

- **Misc**: Support for Python 3.8 by importing from `collections.abc`.

- **Misc**: Got rid of :pypi:`aiohttp` deprecation warnings.

- **Documentation and examples**: Improvements contributed by:

    - Martin Maillard (:github_user:`martinmaillard`).
    - Omar Rayward (:github_user:`omarrayward`).
