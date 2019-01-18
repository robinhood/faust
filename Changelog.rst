.. _changelog:

==============================
 Change history for Faust 1.5
==============================

This document contain change notes for bugfix releases in
the Faust 1.5 series. If you're looking for previous releases,
please visit the :ref:`history` section.

.. _version-1.5.0:

1.5.0
=====
:release-date: TBA
:release-by: TBA

- **Requirements**

    - Now depends on :pypi:`robinhood-aiokafka` 0.5.1

- **App**: New :setting:`consumer_auto_offset_reset` setting (Issue #267).

    Contributed by Ryan Whitten (:github_user:`rwhitten577`).

- **App**: Web server is no longer running in a separate thread by default.

    Running the web server in a separate thread is beneficial as it
    will not be affected by backpressue in the main thread event loop,
    but it also makes programming harder when it cannot share the loop
    of the parent.

    If you want to run the web server in a separate thread, use the new
    :setting:`web_in_thread` setting.

- **App**: New :setting:`web_in_thread` controls separate thread for web
  server.

- **App**: New :setting:`logging_config` setting.

- **App**: Autodiscovery now ignores modules matching "*test*" (Issue #242).

    Contributed by Chris Seto (:github_user:`chrisseto`).

- **Agent**: Fixes crash when worker assigned no partitions and having
  the ``isolated_partitions`` flag enabled (Issue #181).

- **Table**: Fixes :exc:`KeyError` crash for already removed key.

- **Table**: WindowRange is no longer a :class:`~typing.NamedTuple`.

    This will make it easier to avoid hashing mistakes such that
    window ranges are never represented as both normal tuple and named tuple
    variants in the table.

- **Transports**: Adds experimental ``confluent://`` transport.

    This transport uses the :pypi:`confluent-kafka` client.

    It is not feature complete, and notably is missing sticky partition
    assignment so you should not use this transport for tables.

    .. warning::

        The ``confluent://`` transport is not recommended for production
        use at this time as it has several limitations.

- **Stream**: Fixed deadlock when using ``Stream.take`` to buffer events
  (Issue #262).

    Contributed by Nimi Wariboko Jr (:github_user:`nemosupremo`).

- **Stream**: Fixed acking behavior of ``Stream.take`` (Issue #266).

    When ``take`` is buffering the events should be acked after processing
    the buffer is complete, instead it was acking when adding into the buffer.

    Fix contributed by Amit Ripshtos (:github_user:`amitripshtos`).

- **Typing**: Added type stubs for ``faust.web.Request``.

- **Typing**: Fixed type stubs for ``@app.agent`` decorator.

- **Documentation**: Improvements by:

    + Amit Rip (:github_user:`amitripshtos`).
    + Sebastian Roll (:github_user:`SebastianRoll`).
    + Mousse (:github_user:`zibuyu1995`).
