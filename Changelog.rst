.. _changelog:

==============================
 Changes
==============================

This document contain change notes for bugfix releases in
the Faust 1.7 series. If you're looking for previous releases,
please visit the :ref:`history` section.

.. _version-1.7.0:

1.7.0
=====
:release-date: TBA
:release-by: TBA

.. _v170-backward-incompatible-changes:

Backward Incompatible Changes
-----------------------------

- **Transports**: The in-memory transport has been removed (Issue #295).

    This transport was experimental and not working properly, so to avoid
    confusion we have removed it completely.

- **Stream**: The ``Message.stream_meta`` attribute has been removed.

    This was used to keep arbitrary state for sensors during processing
    of a message.

    If you by rare chance are relying on this attribute to exist, you must
    now initialize it before using it:

    .. sourcecode:: python

        stream_meta = getattr(event.message, 'stream_meta', None)
        if stream_meta is None:
            stream_meta = event.message.stream_meta = {}

.. _v170-news:

News
----

- **Requirements**

    + Now depends on :ref:`Mode 4.0.0 <mode:version-4.0.0>`.

    + Now depends on :pypi:`aiohttp` 3.5.2 or later.

        Thanks to :github_user:`CharAct3`.

- **Documentation**: Documented a new deployment strategy to minimize
  rebalancing issues.

    See :ref:`worker-cluster` for more information.

- **Models**: Implements model validation.

    Validation of fields can be enabled by using the ``validation=True`` class
    option:

    .. sourcecode:: python

        import faust
        from decimal import Decimal

        class X(faust.Record, validation=True):
            name: str
            amount: Decimal

    When validation is enabled, the model will validate that the
    fields values are of the correct type.

    Fields can now also have advanced validation options,
    and you enable these by writing explicit field descriptors:

    .. sourcecode:: python

        import faust
        from decimal import Decimal
        from faust.models.fields import DecimalField, StringField

        class X(faust.Record, validation=True):
            name: str = StringField(max_length=30)
            amount: Decimal = DecimalField(min_value=10.0, max_value=1000.0)

    If you want to run validation manually, you can do so by
    keeping ``validation=False`` on the class, but calling
    ``model.is_valid()``:

    .. sourcecode:: python

        if not model.is_valid():
            print(model.validation_errors)

- **Models**: Implements generic coercion support.

    This new feature replaces the ``isodates=True``/``decimals=True`` options
    and can be enabled by passing ``coerce=True``:

    .. sourcecode:: python

        class Account(faust.Record, coerce=True):
            name: str
            login_times: List[datetime]

- **Topic**: Adds new ``topic.send_soon()`` non-async method to buffer
  messages.

    This method can be used by any non-`async def` function
    to buffer up messages to be produced.

    It returns `Awaitable[RecordMetadata]`: a promise evaluated once
    the message is actually sent.

- **Stream**: New ``Stream.filter`` method added useful for filtering
  events before repartitioning a stream.

    See :ref:`stream-filter` for more information.

- **App**: New :setting:`broker_consumer`/:setting:`broker_producer` settings.

    These can now be used to configure individual transports
    for consuming and producing.

    The default value for both settings are taken from the
    :setting:`broker` setting.

    For example you can use :pypi:`aiokafka` for the consumer, and
    :pypi:`confluent_kafka` for the producer:

    .. sourcecode:: python

        app = faust.App(
            'id',
            broker_consumer='kafka://localhost:9092',
            broker_producer='confluent://localhost:9092',
        )

- **App**: New :setting:`broker_max_poll_interval` setting.

  Contributed by Miha Troha (:github_user:`mihatroha`).

- **App**: New :setting:`topic_disable_leader` setting disables
  the leader topic.

- **Table**: Table constructor now accepts ``options`` argument
  passed on to underlying RocksDB storage.

    This can be used to configure advanced RocksDB options,
    such as block size, cache size, etc.

    Contributed by Miha Troha (:github_user:`mihatroha`).

.. _v170-fixes:

Fixes
-----

- **Stream**: Fixes bug where non-finished event is acked (Issue #355).

- **Producer**: Exactly once: Support producing to non-transactional
  topics (Issue #339)

- **Agent**: Test: Fixed :exc:`asyncio.CancelledError` (Issue #322).

- **Cython**: Fixed issue with sensor state not being passed to ``after``.

- **Tables**: Key index: now inherits configuration from source table
  (Issue #325)

- **App**: Fix list of strings for :setting:`broker` param in URL
  (Issue #330).

    Contributed by Nimish Telang (:github_user:`nimish`).

- **Table**: Fixed blocking behavior when populating tables.

    Symptom was warnings about timers waking up too late.

- **Documentation** Fixes by:

    + :github_user:`evanderiel`

.. _v170-improvements:

Improvements
------------

- **Documentation**: Rewrote fragmented documentation to be more concise.

- **Documentation improvements by**

    + Igor Mozharovsky (:github_user:`seedofjoy`)

    + Stephen Sorriaux (:github_user:`StephenSorriaux`)

    + Lifei Chen (:github_user:`hustclf`)
