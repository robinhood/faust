.. _changelog-1.4:

==============================
 Change history for Faust 1.4
==============================

This document contain change notes for bugfix releases in
the Faust 1.4.x series. If you're looking for changes in the latest
series, please visit the latest :ref:`changelog`.

For even older releases you can visit the :ref:`history` section.

.. _version-1.4.9:

1.4.9
=====
:release-date: 2019-03-14 04:00 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

    + Now depends on :ref:`Mode 3.0.10 <mode:version-3.0.10>`.

- :setting:`max_poll_records` accidentally set to 500 by default.

    The setting has been reverted to its documented default of :const:`None`.
    This resulted in a 20x performance improvement :tada:

- **CLI**: Now correctly returns non-zero exitcode when exception raised
  inside ``@app.command``.

- **CLI**: Option ``--no_color`` renamed to ``--no-color``
  to be consistent with other options.

    This change is backwards compatible and ``--no_color`` will continue to
    work.

- **CLI**: The ``model x`` command used "default*" as the field name
  for default value.

    .. sourcecode:: console

        $ python examples/withdrawals.py --json model Withdrawal | python -m json.tool
        [
            {
                "field": "user",
                "type": "str",
                "default*": "*"
            },
            {
                "field": "country",
                "type": "str",
                "default*": "*"
            },
            {
                "field": "amount",
                "type": "float",
                "default*": "*"
            },
            {
                "field": "date",
                "type": "datetime",
                "default*": "None"
            }
        ]

    This now gives "default" without the extraneous star.

- **App**: Can now override the settings class used.

    This means you can now easily extend your app with custom settings:

    .. sourcecode:: python

        import faust

        class MySettings(faust.Settings):
            foobar: int

            def __init__(self, id: str, *, foobar: int = 0, **kwargs) -> None:
                super().__init__(id, **kwargs)
                self.foobar = foobar

        class App(faust.App):
            Settings = MySettings

        app = App('id', foobar=3)
        print(app.conf.foobar)

.. _version-1.4.8:

1.4.8
=====
:release-date: 2019-03-11 05:30 P.M PDT
:release-by: Ask Solem (:github_user:`ask`)

- **Tables**: Recovery would hang when changelog have
  ``committed_offset == 0``.

    Added this test to our manual testing procedure.

.. _version-1.4.7:

1.4.7
=====
:release-date: 2019-03-08 02:21 P.M PDT
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

    + Now depends on :ref:`Mode 3.0.9 <mode:version-3.0.9>`.

- **Tables**: Read offset not always updated after seek
  caused recovery to hang.

- **Consumer**: Fix to make sure fetch requests will not block method queue.

- **App**: Fixed deadlock in rebalancing.

- **Web**: Views can now define ``options`` method to
  implement a handler for the HTTP ``OPTIONS`` method.
  (Issue #304)

    Contributed by Perk Lim (:github_user:`perklun`).

- **Web**: Can now pass headers to HTTP responses.

.. _version-1.4.6:

1.4.6
=====
:release-date: 2019-01-29 01:52 P.M PDT
:release-by: Ask Solem (:github_user:`ask`)

- **App**: Better support for custom boot strategies by having
  the app start without waiting for recovery when no tables started.

- **Docs**: Fixed doc build after intersphinx
     URL https://click.palletsprojects.com/en/latest no longer works.

.. _version-1.4.5:

1.4.5
=====
:release-date: 2019-01-18 02:15 P.M PDT
:release-by: Ask Solem (:github_user:`ask`)

- Fixed typo in 1.4.4 release (on_recovery_set_flags -> on_rebalance_start).

.. _version-1.4.4:

1.4.4
=====
:release-date: 2019-01-18 01:10 P.M PDT
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

    + Now depends on :ref:`Mode 3.0.7 <mode:version-3.0.7>`.

- **App**: App now starts even if there are no agents defined.

- **Table**: Added new flags to detect if actives/standbys are ready.

    - ``app.tables.actives_ready``

        Set to :const:`True` when active tables are recovered from
        and are ready to use.

    - ``app.tables.standbys_ready``

        Set to :const:`True` when standbys are up to date after
        recovery.

.. _version-1.4.3:

1.4.3
=====
:release-date: 2019-01-14 03:01 P.M PDT
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

  + Require series 0.4.x of :pypi:`robinhood-aiokafka`.

    - Recently version 0.5.0 was released but this has not been tested
      in production yet, so we have pinned Faust 1.4.x to aiokafka 0.4.x.
      For more information see Issue #277.

  + Test requirements now depends on :pypi:`pytest` greater than 3.6.

    Contributed by Michael Seifert (:github_user:`seifertm`).

- **Documentation improvements by**:

    + Allison Wang (:github_user:`allisonwang`).
    + Thibault Serot (:github_user:`thibserot`).
    + oucb (:github_user:`oucb`).

- **CI**: Added CPython 3.7.2 and 3.6.8 to Travis CI build matrix.

.. _version-1.4.2:

1.4.2
=====
:release-date: 2018-12-19 12:49 P.M PDT
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

    + Now depends on :ref:`Mode 3.0.5 <mode:version-3.0.5>`.

        Fixed compatibility with :pypi:`colorlog`,
        thanks to Ryan Whitten (:github_user:`rwhitten577`).

    + Now compatible with :pypi:`yarl` 1.3.x.

- **Agent**: Allow ``yield`` in agents that use ``Stream.take`` (Issue #237).

- **App**: Fixed error "future for different event loop" when web views
           send messages to Kafka at startup.

- **Table**: Table views now return HTTP 503 status code during startup
  when table routing information not available.

- **App**: New ``App.BootStrategy`` class now decides what services
  are started when starting the app.

- Documentation fixes by:

    - Robert Krzyzanowski (:github_user:`robertzk`).

.. _version-1.4.1:

1.4.1
=====
:release-date: 2018-12-10 4:49 P.M PDT
:release-by: Ask Solem (:github_user:`ask`)

- **Web**: Disable :pypi:`aiohttp` access logs for performance.

.. _version-1.4.0:

1.4.0
=====
:release-date: 2018-12-07 4:29 P.M PDT
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

    + Now depends on :ref:`Mode 3.0 <mode:version-3.0.0>`.

- **Worker**: The Kafka consumer is now running in a separate thread.

    The Kafka heartbeat background corutine sends heartbeats every 3.0 seconds,
    and if those are missed rebalancing occurs.

    This patch moves the :pypi:`aiokafka` library inside a separate thread,
    this way it can send responsive heartbeats and operate even when agents
    call blocking functions such as ``time.sleep(60)`` for every event.

- **Table**: Experimental support for tables where values are sets.

    The new ``app.SetTable`` constructor creates a table where values are sets.
    Example uses include keeping track of users at a location:
    ``table[location].add(user_id)``.

    Supports all set operations: ``add``, ``discard``, ``intersection``,
    ``union``, ``symmetric_difference``, ``difference``, etc.

    Sets are kept in memory for fast operation, and this way we also avoid
    the overhead of constantly serializing/deserializing the data to RocksDB.
    Instead we periodically flush changes to RocksDB, and populate the sets
    from disk at worker startup/table recovery.

- **App**: Adds support for crontab tasks.

    You can now define periodic tasks using cron-syntax:

    .. sourcecode:: python

        @app.crontab('*/1 * * * *', on_leader=True)
        async def publish_every_minute():
            print('-- We should send a message every minute --')
            print(f'Sending message at: {datetime.now()}')
            msg = Model(random=round(random(), 2))
            await tz_unaware_topic.send(value=msg).

    See :ref:`tasks-cron-jobs` for more information.

    Contributed by Omar Rayward (:github_user:`omarrayward`).

- **App**: Providing multiple URLs to the :setting:`broker` setting
  now works as expected.

    To facilitiate this change ``app.conf.broker`` is now
    ``List[URL]`` instead of a single :class:`~yarl.URL`.

- **App**: New :setting:`timezone` setting.

    This setting is currently used as the default timezone for crontab tasks.

- **App**: New :setting:`broker_request_timeout` setting.

    Contributed by Martin Maillard (:github_user:`martinmaillard`).

- **App**: New :setting:`broker_max_poll_records` setting.

    Contributed by Alexander Oberegger (:github_user:`aoberegg`).

- **App**: New :setting:`consumer_max_fetch_size` setting.

    Contributed by Matthew Stump (:github_user:`mstump`).

- **App**: New :setting:`producer_request_timeout` setting.

    Controls when producer batch requests expire, and when we give up
    sending batches as producer requests fail.

    This setting has been increased to 20 minutes by default.

- **Web**: :pypi:`aiohttp` driver now uses ``AppRunner`` to start the web
  server.

    Contributed by Mattias Karlsson (:github_user:`stevespark`).

- **Agent**: Fixed RPC example (Issue #155).

    Contributed by Mattias Karlsson (:github_user:`stevespark`).

- **Table**: Added support for iterating over windowed tables.

    See :ref:`windowed-table-iter`.

    This requires us to keep a second table for the key index, so support
    for windowed table iteration requires you to set a ``use_index=True``
    setting for the table:

    .. sourcecode:: python

        windowed_table = app.Table(
            'name',
            default=int,
        ).hopping(10, 5, expires=timedelta(minutes=10), key_index=True)

    After enabling the ``key_index=True`` setting you may iterate over
    keys/items/values in the table:

    .. sourcecode:: python

        for key in windowed_table.keys():
            print(key)

        for key, value in windowed_table.items():
            print(key, value)

        for value in windowed_table.values():
            print(key, value)

    The ``items`` and ``values`` views can also select time-relative
    iteration:

    .. sourcecode:: python

        for key, value in windowed_table.items().delta(30):
            print(key, value)
        for key, value in windowed_table.items().now():
            print(key, value)
        for key, value in windowed_table.items().current():
            print(key, value)

- **Table**: Now raises error if source topic has mismatching
   number of partitions with changelog topic. (Issue #137).

- **Table**: Allow using raw serializer in tables.

    You can now control the serialization format for changelog tables,
    using the ``key_serializer`` and ``value_serializer`` keyword
    arguments to ``app.Table(...)``.

    Contributed by Matthias Wutte (:github_user:`wuttem`).

- **Worker**: Fixed spinner output at shutdown.

- **Models**: ``isodates`` option now correctly parses
  timezones without separator such as `-0500`.

- **Testing**: Calling ``AgentTestWrapper.put`` now propagates exceptions
  raised in the agent.

- **App**: Default value for :setting:`stream_recovery_delay` is now 3.0
  seconds.

- **CLI**: New command "clean_versions" used to delete old version directories
  (Issue #68).

- **Web**: Added view decorators: ``takes_model`` and ``gives_model``.
