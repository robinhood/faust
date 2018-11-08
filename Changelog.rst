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
:release-date: TBA
:release-by: TBA (:github_user:`TBA`)

- **Requirements**

    + Now depends on :ref:`Mode 2.0.3 <mode:version-2.0.3>`.

    + Now depends on :mod:`robinhood-aiokafka` 1.4.19

- **App**: Refactored rebalancing and table recovery (Issue #185).

    This optimizes the rebalancing callbacks for greater stability.

    Table recovery was completely rewritten to do as little as possible
    during actual rebalance.  This should increase stability and reduce
    the chance of rebalancing loops.

- **Windowing**: Fixed several edge cases in windowed tables.

    Fix contributed by Omar Rayward (:github_user:`omarrayward`).

- **App**: Skip table recovery on rebalance when no tables defined.

- **RocksDB**: Iterating over table keys/items/values now skips
  standby partitions.

- **App**: Adds :signal:`App.on_rebalance_complete` signal.

- **App**: Adds :signal:`App.on_before_shutdown` signal.
