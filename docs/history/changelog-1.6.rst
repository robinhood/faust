.. _changelog-1.6:

==============================
 Change history for Faust 1.6
==============================

This document contain change notes for bugfix releases in
the Faust 1.6.x series. If you're looking for changes in the latest
series, please visit the latest :ref:`changelog`.

For even older releases you can visit the :ref:`history` section.

.. _version-1.6.1:

1.6.1
=====
:release-date: 2019-05-07 2:00 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Web**: Fixes index page of web server by adding :class:`collections.deque`
  support to our JSON serializer.

    Thanks to Brandon Ewing for detecting this issue.

.. _version-1.6.0:

1.6.0
=====
:release-date: 2019-04-16 5:41 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

This release has minor backward incompatible changes.
that only affects those who are using custom sensors.
See note below.

- **Requirements**:

    + Now depends on :pypi:`robinhood-aiokafka` 1.0.3

        This version disables the "LeaveGroup" timeout
        added in 1.0.0, as it was causing problems.

- **Sensors**: ``on_stream_event_in`` now passes state to
  ``on_stream_event_out``.

    This is backwards incompatible but fixes a rare race condition.

    Custom sensors that have to use stream_meta must be updated
    to use this state.

- **Sensors**: Added new sensor methods:

    + ``on_rebalance_start(app)``

        Called when a new rebalance is starting.

    + ``on_rebalance_return(app)``

        Called when the worker has returned data to Kafka.

        The next step of the rebalancing phase will be the
        table recovery process, but this happens in the background
        and rebalancing will be considered complete for this worker.

    + ``on_rebalance_end(app)``

        Called when all tables are fully recovered
        and the worker is ready to start processing events in the stream.

- **Sensors**: The type of a sensor that returns/takes state is now
  Dict instead of a Mapping (as the state is mutable).

- **Monitor**: Optimized latency history cleanup.

- **Recovery**: Fixed bug with highwater returning :const:`None`.

- **Tracing**: The ``traced`` decorator would return :const:`None`
  for wrapped coroutines, but we now return the actual return value.

- **Tracing**: Added tracing of :pypi:`aiokafka` group coordinator processes
   (rebalancing and find coordinator).

