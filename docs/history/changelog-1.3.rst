.. _changelog-1.3:

==============================
 Change history for Faust 1.3
==============================

This document contain change notes for bugfix releases in
the Faust 1.3.x series. If you're looking for changes in the latest
series, please visit the latest :ref:`changelog`.

For even older releases you can visit the :ref:`history` section.

.. _version-1.3.2:

1.3.2
=====
:release-date: 2018-11-19 1:11 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

    + Now depends on :ref:`Mode 2.0.4 <mode:version-2.0.4>`.

- Fixed crash in ``perform_seek`` when worker was not assigned any partitions.

- Fixed missing ``await`` in ``Consumer.wait_empty``.

- Fixed hang after rebalance when not using tables.

.. _version-1.3.1:

1.3.1
=====
:release-date: 2018-11-15 4:12 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Tables**: Fixed problem with table recovery hanging on
  changelog topics having only a single entry.

.. _version-1.3.0:

1.3.0
=====
:release-date: 2018-11-08 4:49 P.M PST
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

- **App**: Allow :setting:`broker` URL setting without scheme.

    The default scheme for an URL like "localhost:9092" is ``kafka://``.

- **App**: Adds :signal:`App.on_rebalance_complete` signal.

- **App**: Adds :signal:`App.on_before_shutdown` signal.

- **Misc**: Support for Python 3.8 by importing from `collections.abc`.

- **Misc**: Got rid of :pypi:`aiohttp` deprecation warnings.

- **Documentation and examples**: Improvements contributed by:

    - Martin Maillard (:github_user:`martinmaillard`).
    - Omar Rayward (:github_user:`omarrayward`).
