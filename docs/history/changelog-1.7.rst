.. _changelog-1.7:

==============================
 Change history for Faust 1.7
==============================

This document contain change notes for bugfix releases in
the Faust 1.7.x series. If you're looking for changes in the latest
series, please visit the latest :ref:`changelog`.

For even older releases you can visit the :ref:`history` section.

.. _version-1.7.3:

1.7.3
=====
:release-date: 2019-07-12 1:13 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Tables**: Fix for Issue #383 when using the Cython extension.

.. _version-1.7.2:

1.7.2
=====
:release-date: 2019-07-12 12:00 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Tables**: Fixed memory leak/back pressure in changelog producer buffer
  (Issue #383)

- **Models**: Do not attempt to parse datetime when coerce/isodates disabled.

    Version 1.7 introduced a regression where datetimes were attempted
    to be parsed as ISO-8601 even with the ``isodates`` setting disabled.

    A regression test was added for this bug.

- **Models**: New ``date_parser`` option to change datetime parsing function.

    The default date parser supports ISO-8601 only.  To support
    this format and many other formats (such as
    ``'Sat Jan 12 00:44:36 +0000 2019'``) you can select to
    use :pypi:`python-dateutil` as the parser.

    To change the date parsing function for a model globally:

    .. sourcecode:: python

        from dateutil.parser import parse as parse_date

        class Account(faust.Record, coerce=True, date_parser=parse_date):
            date_joined: datetime

    To change the date parsing function for a specific field:

    .. sourcecode:: python

        from dateutil.parser import parse as parse_date
        from faust.models.fields import DatetimeField

        class Account(faust.Record, coerce=True):
            # date_joined: supports ISO-8601 only (default)
            date_joined: datetime

            #: date_last_login: comes from weird system with more human
            #: readable dates ('Sat Jan 12 00:44:36 +0000 2019').
            #: The dateutil parser can handle many different date and time
            #: formats.
            date_last_login: datetime = DatetimeField(date_parser=parse_date)

- **Models**: Adds ``FieldDescriptor.exclude`` to exclude field when serialized

    See :ref:`model-field-exclude` for more information.

- **Documentation**: improvements by...

  + Witek Bedyk (:github_user:`witekest`).
  + Josh Woodward (:github_user:`jdw6359`).

.. _version-1.7.1:

1.7.1
=====
:release-date: 2019-07-09 2:36 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Stream**: Exactly once processing now include the app id
  in transactional ids.

    This was done to support running multiple apps on the same
    Kafka broker.

    Contributed by Cesar Pantoja (:github_user:`CesarPantoja`).

- **Web**: Fixed bug where sensor index should display when :setting:`debug` is enabled

    .. tip::

        If you want to enable the sensor statistics endpoint in production,
        without enabling the :setting:`debug` setting, you can do so
        by adding the following code:

        .. sourcecode:: python

            app.web.blueprints.add('/stats/', 'faust.web.apps.stats:blueprint')

    Contributed by :github_user:`tyong920`

- **Transport**: The default value for :setting:`broker_request_timeout` is now
  90 seconds (Issue #259)

- **Transport**: Raise error if :setting:`broker_session_timeout` is greater
  than :setting:`broker_request_timeout` (Closes #259)

- **Dependencies**: Now supports :pypi:`click` 7.0 and later.

- **Dependencies**: ``faust[debug]`` now depends on :pypi:`aiomonitor` 0.4.4
  or later.

- **Models**: Field defined as ``Optional[datetime]`` now works with
  ``coerce`` and ``isodates`` settings.

    Previously a model would not recognize:

    .. sourcecode:: python

        class X(faust.Record, coerce=True):
            date: Optional[datetime]

    as a :class:`~faust.models.fields.DatetimeField` and when
    deserializing the field would end up as a string.

    It's now properly converted to :class:`~datetime.datetime`.

- **RocksDB**: Adds :setting:`table_key_index_size` setting (Closes #372)

- **RocksDB**: Reraise original error if :pypi:`python-rocksdb` cannot
  be imported.

    Thanks to Sohaib Farooqi.

- **Django**: Autodiscovery support now waits for Django to be fully setup.

    Contributed by Tomasz Nguyen (:github_user:`swist`).

- **Documentation** improvements by:

  + Witek Bedyk (:github_user:`witekest`).

.. _version-1.7.0:

1.7.0
=====
:release-date: 2019-06-06 6:00 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

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

- **Testing**: New experimental ``livecheck`` production testing API.

    There is no documentation yet, but an example in
    ``examples//livecheck.py``.

    This is a new API to do end-to-end testing directly in production.

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
