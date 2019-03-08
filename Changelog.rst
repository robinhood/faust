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

    + Now depends on :pypi:`robinhood-aiokafka` 0.5.2

    + Now depends on :ref:`Mode 3.0.8 <mode:version-3.0.8>`.

- **App**: Experimental support for "exactly-once" semantics.

    This mode ensures tables and counts in tables/windows are consistent
    even as nodes in the cluster are abruptly terminated.

    To enable this mode set the :setting:`processing_guarantee` setting:

    .. sourcecode:: python

        App(processing_guarantee='exactly_once')

- **App**: Sending messages API now supports a ``timestamp`` argument
  (Issue #276).

    When sending messages you can now specify the timestamp
    of the message:

    .. sourcecode:: python

        await topic.send(key=key, value=value, timestamp=custom_timestamp)

    If no timestamp is provided the current time will be used
    (:func:`time.time`).

    Contributed by Miha Troha (:github_user:`mihatroha`).

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

- **RocksDB**: ``len(table)`` now only counts databases for active partitions
  (Issue #270).

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

- **Web**: Views can now define ``options`` method to
  implement a handler for the HTTP ``OPTIONS`` method.
  (Issue #304)

    Contributed by Perk Lim (:github_user:`perklun`).

- **Stream**: Fixed acking behavior of ``Stream.take`` (Issue #266).

    When ``take`` is buffering the events should be acked after processing
    the buffer is complete, instead it was acking when adding into the buffer.

    Fix contributed by Amit Ripshtos (:github_user:`amitripshtos`).

- **Transport**: Aiokafka was not limiting how many messages to read in
   a fetch request (Issue #292).

    Fix contributed by Miha Troha (:github_user:`mihatroha`).

- **Typing**: Added type stubs for ``faust.web.Request``.

- **Typing**: Fixed type stubs for ``@app.agent`` decorator.

- **Debugging**: Added `OpenTracing`_ hooks to streams/tasks/timers/crontabs
   and rebalancing process.

    To enable you have to define a custom ``Tracer`` class that will
    record and publish the traces to systems such as `Jeager`_ or `Zipkin`_.

    This class needs to have a ``.trace(name, **extra_context)`` context
    manager:

    .. sourcecode:: python

        import opentracing
        from typing import Any, ContextManager
        from faust.types.core import HeadersArg, merge_headers

        class Tracer:

            @contextmanager
            def trace(self, name: str, request_headers: Mapping,
                      **extra_context: Any) -> ContextManager:
                # request headers contain the uber-trace-id if any,
                # you can use opentracing.Tracer.extract.

                tracer: opentracing.Tracer = CREATE_TRACER()
                with tracer.start_span(name) as span:
                    for key, value in extra_context.items():
                        span.set_tag(key, value)
                    yield

            def trace_inject_headers(self, headers: HeadersArg) -> HeadersArg:
                span: opentracing.Span = GET_CURRENT_SPAN()
                if span is not None:
                    carrier = {}
                    tracer: opentracing.Tracer = span.tracer
                    tracer.inject(
                        span_context=span,
                        format=opentracing.Format.HTTP_HEADERS,
                        carrier=carrier,
                    )
                    return merge_headers(headers, carrier)
                return headers


    After implementing the interface you need to set the ``app.tracer``
    attribute:

    .. sourcecode:: python

        app = faust.App(...)
        app.tracer = Tracer()

    That's it! Now traces will go through your custom tracing implementation.

.. _`OpenTracing`: https://opentracing.io
.. _`Jeager`: https://www.jaegertracing.io
.. _`Zipkin`: https://zipkin.io

- **Documentation**: Improvements by:

    + Amit Rip (:github_user:`amitripshtos`).
    + Sebastian Roll (:github_user:`SebastianRoll`).
    + Mousse (:github_user:`zibuyu1995`).
