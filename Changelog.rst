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

    + Now depends on :ref:`Mode 3.0.11 <mode:version-3.0.11>`.

- New optimizations for stream processing and windows.

    If Cython is available during installation, Faust will be installed
    with compiled extensions.

    You can set the :envvar:`NO_CYTHON` environment variable
    to disable the use of these extensions even if compiled.

- Exactly-Once semantics: New :setting:`processing_guarantee` setting.

    Experimental support for "exactly-once" semantics.

    This mode ensures tables and counts in tables/windows are consistent
    even as nodes in the cluster are abruptly terminated.

    To enable this mode set the :setting:`processing_guarantee` setting:

    .. sourcecode:: python

        App(processing_guarantee='exactly_once')

- New :setting:`topic_allow_declare` setting.

    If disabled your faust worker instances will never actually
    declare topics.

    Use this if your Kafka administrator does not allow you to
    create topics.

- New :setting:`ConsumerScheduler` setting.

    This class can override how events are delivered to agents.
    The default will go round robin between both topics and partitions,
    to ensure all topic partitions get a chance of being processed.

    Contributed by Miha Troha (:github_user:`miatroha`).

- **Models**: Records can now use comparison operators.

    Comparison of models using the ``>``, ``<``, ``>=`` and ``<=`` operators
    now work similarly to :mod:`dataclasses`.

- **Models**: Now raise an error if non-default fields follows default fields.

    The following model will now raise an error:

    .. sourcecode:: python

        class Account(faust.Record):
            name: str
            amount: int = 3
            userid: str

    This is because a non-default field is defined after a default field,
    and this would mess up argument ordering.

    To define the model without error, make sure you move default fields
    below any non-default fields:

    .. sourcecode:: python

        class Account(faust.Record):
            name: str
            userid: str
            amount: int = 3

    .. note::

        Remember that when adding fields to an already existing model
        you should always add new fields as optional fields.

        This will help your application stay backward compatible.

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

        from typing import Any, Dict,
        import opentracing
        from opentracing.ext.tags import SAMPLING_PRIORITY

        class FaustTracer:
            _tracers: Dict[str, opentracing.Tracer]
            _default_tracer: opentracing.Tracer = None

            def __init__(self) -> None:
                self._tracers = {}

            @cached_property
            def default_tracer(self) -> opentracing.Tracer:
                if self._default_tracer is None:
                    self._default_tracer = self.get_tracer('APP_NAME')

            def trace(self, name: str,
                      sample_rate: float = None,
                      **extra_context: Any) -> opentracing.Span:
                    span = self.default_tracer.start_span(
                    operation_name=name,
                    tags=extra_context,
                )

                if sample_rate is not None:
                    priority = 1 if random.uniform(0, 1) < sample_rate else 0
                    span.set_tag(SAMPLING_PRIORITY, priority)
                return span

            def get_tracer(self, service_name: str) -> opentracing.Tracer:
                tracer = self._tracers.get(service_name)
                if tracer is None:
                    tracer = self._tracers[service_name] = CREATE_TRACER(service_name)
                return tracer._tracer

    After implementing the interface you need to set the ``app.tracer``
    attribute:

    .. sourcecode:: python

        app = faust.App(...)
        app.tracer = FaustTracer()

    That's it! Now traces will go through your custom tracing implementation.

.. _`OpenTracing`: https://opentracing.io
.. _`Jeager`: https://www.jaegertracing.io
.. _`Zipkin`: https://zipkin.io

- **CLI**: Commands ``--help`` output now always show the default for
  every parameter.

- **Channels**: Fixed bug in ``channel.send`` that caused a memory leak.

    This bug was not present when using ``app.topic()``.

- **Documentation**: Improvements by:

    + Amit Rip (:github_user:`amitripshtos`).
    + Sebastian Roll (:github_user:`SebastianRoll`).
    + Mousse (:github_user:`zibuyu1995`).
    + Zhanzhao (Deo) Liang (:github_user:`DeoLeung`).

- **Testing**:

    - 99% total unit test coverage
    - New script to verify documentation defaults are up to date are
      run for every git commit.

