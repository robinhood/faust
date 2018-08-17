.. _changelog-1.0:

==============================
 Change history for Faust 1.0
==============================

This document contain change notes for bugfix releases in
the Faust 1.x series. If you're looking for changes in the latest
series, please visit the latest :ref:`changelog`.

For even older releases you can visit the :ref:`history` section.

.. _version-1.0.30:

1.0.30
======
:release-date: 2018-08-15 3:17 P.M PDT
:release-by: Ask Solem

- **Requirements**

    + Now depends on :ref:`Mode 1.15.1 <mode:version-1.15.1>`.

- **Typing**: :attr:`faust.types.Message.timestamp_type` is now the correct
              :class:`int`, previously it was string by message.

- **Models**: Records can now have recursive fields.

    For example a tree structure model having a field that refers back to
    itself:

    .. sourcecode:: python

        class Node(faust.Record):
            data: Any
            children: List['Node']

- **Models**: A field of type ``List[Model]`` no longer raises an exception
              if the value provided is :const:`None`.

- **Models**: Adds support for ``--strict-optional``-style fields.

    Previously the following would work:

    .. sourcecode:: python

        class Order(Record):
            account: Account = None

    The account is considered optional from a typing point of view, but only
    if the :pypi:`mypy` option ``--strict-optional`` is disabled.

    Now that ``--strict-optional`` is enabled by default in :pypi:`mypy`,
    this version adds support for fields such as:

    .. sourcecode:: python

        class Order(Record):
            account: Optional[Account] = None
            history: Optional[List[OrderStatus]]

- **Models**: Class options such as ``isodates``/``include_metadata``/etc. are
              now inherited from parent class.

- **Stream**: Fixed :exc:`NameError` when pushing non-Event value into stream.

.. _version-1.0.29:

1.0.29
======
:release-date: 2018-08-10 5:00 P.M PDT
:release-by: Vineet Goel

- **Requirements**

    + Now depends on :pypi:`robinhood-aiokafka` 0.4.18

        The coordination routine now ensures the program stops
        when receiving a :exc:`aiokafka.errors.UnknownError` from the
        Kafka broker. This leaves recovery up to the supervisor.

- **Table**: Fixed hanging at startup/rebalance on Python 3.7 (Issue #134).

    Workaround for :mod:`asyncio` bug seemingly introduced in Python 3.7,
    that left the worker hanging at startup when attempting to recover
    a table without any data.

- **Monitor**: More efficient updating of highwater metrics (Issue #139).

- **Partition Assignor**: The assignor now compresses the metadata being
  passed around to all application instances for efficiency and to avoid
  extreme cases where the metadata is too big.

.. _version-1.0.28:

1.0.28
======
:release-date: 2018-08-08 11:25 P.M PDT
:release-by: Vineet Goel

- **Monitor**: Adds consumer stats such as last read offsets, last committed
  offsets and log end offsets to the monitor. Also added to the StatsdMonitor.

- **aiokafka**: Changes how topics are created to make it more efficient. We
  now are smarter about finding kafka cluster controller instead of trial and
  error.

- **Documentation**: Fixed links to Slack and other minor fixes.

.. _version-1.0.27:

1.0.27
======
:release-date: 2018-07-30 04:00 P.M PDT
:release-by: Ask Solem

- No code changes

- Fixed links to documentation in README.rst

.. _version-1.0.26:

1.0.26
======
:release-date: 2018-07-30 08:00 A.M PDT
:release-by: Ask Solem

- Public release.

.. _version-1.0.25:

1.0.25
======
:release-date: 2018-07-27 12:43 P.M PDT
:release-by: Ask Solem

- :setting:`stream_publish_on_commit` accidentally disabled by default.

    This made the rate of producing much slower, as the default buffering
    settings are not optimized.

- The ``App.rebalancing`` flag is now reset after the tables have
  recovered.

.. _version-1.0.24:

1.0.24
======
:release-date: 2018-07-12 6:54 P.M PDT
:release-by: Ask Solem

- **Requirements**

    + Now depends on :pypi:`robinhood-aiokafka` 0.4.17

        This fixed an issue where the consumer would be left hanging
        without a connection to Kafka.

.. _version-1.0.23:

1.0.23
======
:release-date: 2018-07-11 5:00 P.M PDT
:release-by: Ask Solem

- **Requirements**

    + Now depends on :pypi:`robinhood-aiokafka` 0.4.16

- Now compatible with Python 3.7.

- Setting :setting:`stream_wait_empty` is now disabled by default (Issue #117).

- Documentation build now compatible with Python 3.7.

    - Fixed ``ForwardRef has no attribute __origin__`` error.

    - Fixed ``DeprecatedInSphinx2.0`` warnings.

- **Web**: Adds ``app.on_webserver_init(web)`` callback for ability to serve static
  files using ``web.add_static``.

- **Web**: Adds web.add_static(prefix, fs_path)

- **Worker**: New ``App.unassigned`` attribute is now set if the worker
  does not have any assigned partitions.

- **CLI**: Console colors was disabled by default.

.. _version-1.0.22:

1.0.22
======
:release-date: 2018-06-27 5:35 P.M PDT
:release-by: Vineet Goel

- **aiokafka**: Timeout for topic creation now wraps entire topic creation.
  Earlier this timeout was for each individual request.

- **testing**: Added stress testing suite.

.. _version-1.0.21:

1.0.21
======
:release-date: 2018-06-27 1:43 P.M PDT
:release-by: Ask Solem

.. warning::

    This changes the package name of ``kafka`` to ``rhkafka``.

- **Requirements**

    + Now depends on :pypi:`robinhood-aiokafka` 0.4.14

    + Now depends on :ref:`Mode 1.15.0 <mode:version-1.15.0>`.

.. _version-1.0.20:

1.0.20
======
:release-date: 2018-06-26 2:35 P.M PDT
:release-by: Vineet Goel

- **Monitor**: Added ``Monitor.count`` to add arbitrary metrics to app monitor.

- **Statsd Monitor**: Normalize agent metrics by removing memory address to
  avoid spamming statsd with thousands of unique metrics per agent.

.. _version-1.0.19:

1.0.19
======
:release-date: 2018-06-25 6:40 P.M PDT
:release-by: Vineet Goel

- **Assignor**: Fixed crash if initial state of assignment is invalid. This
  was causing the following error: ``ValueError('Actives and Standbys are
  disjoint',).`` during partition assignment.

.. _version-1.0.18:

1.0.18
======
:release-date: 2018-06-21 3:53 P.M PDT
:release-by: Ask Solem

- **Worker**: Fixed ``KeyError: TopicPartition(topic='...', partition=x)``
  occurring during rebalance.

.. _version-1.0.17:

1.0.17
======
:release-date: 2018-06-21 3:15 P.M PDT
:release-by: Ask Solem

- **Requirements**

    + Now depends on :pypi:`robinhood-aiokafka` 0.4.13

- We now raise an error if the official :pypi:`aiokafka` or
  :pypi:`kafka-python` is installed.

    Faust depends on a fork of :pypi:`aiokafka` and can not be installed
    with the official versions of :pypi:`aiokafka` and :pypi:`kafka-python`.

    If you have those in requirements, please remove them from your
    virtual env and remove them from requirements.

- **Worker**: Fixes hanging in wait_empty.

    This should also make rebalances faster.

- **Worker**: Adds timeout on topic creation.

.. _version-1.0.16:

1.0.16
======
:release-date: 2018-06-19 3:46 P.M PDT
:release-by: Ask Solem

- **Worker**: :pypi:`aiokafka` create topic request default timeout now set
              to 20 seconds (previously it was accidentally set to 1000
              seconds).

- **Worker**: Fixes crash from :exc:`AssertionError` where ``table._revivers``
              is an empty list.

- **Distribution**: Adds
  :file:`t/misc/scripts/rebalance/killer-always-same-node.sh`.

.. _version-1.0.15:

1.0.15
======
:release-date: 2018-06-14 7:36 P.M PDT
:release-by: Ask Solem

- **Requirements**

    + Now depends on :pypi:`robinhood-aiokafka` 0.4.12

- **Worker**: Fixed problem where worker does not recover after macbook
  sleeping and waking up.

- **Worker**: Fixed crash that could lead to rebalancing loop.

- **Worker**: Removed some noisy errors that weren't really errors.

.. _version-1.0.14:

1.0.14
======
:release-date: 2018-06-13 5:58 P.M PDT
:release-by: Ask Solem

- **Requirements**

    + Now depends on :pypi:`robinhood-aiokafka` 0.4.11

- **Worker**: :pypi:`aiokafka`'s heartbeat thread would sometimes keep the
  worker alive even though the worker was trying to shutdown.

    An error could have happened many hours ago causing the worker to crash
    and attempt a shutdown, but then the heartbeat thread kept the worker
    from terminating.

    Now the rebalance will check if the worker is stopped and then
    appropriately stop the heartbeat thread.

- **Worker**: Fixed error that caused rebalancing to hang:
  ``"ValueError: Set of coroutines/Futures is empty."``.

- **Worker**: Fixed error "Coroutine x tried to break fence owned by y"

    This was added as an assertion to see if multiple threads would use the
    variable at the same time.

- **Worker**: Removed logged error "not assigned to topics" now that we
  automatically recover from non-existing topics.

- **Tables**: Ignore :exc:`asyncio.CancelledError` while stopping standbys.

- **Distribution**: Added scripts to help stress test rebalancing
  in :file:`t/misc/scripts/rebalance`.

.. _version-1.0.13:

1.0.13
======
:release-date: 2018-06-12 2:10 P.M PDT
:release-by: Ask Solem

- **Worker**: The Kafka fetcher service was taking too long to shutdown
  on rebalance.

    If this takes longer than the session timeout, it triggers another
    rebalance, and if it happens repeatedly this will cause the cluster
    to be in a state of constant rebalancing.

    Now we use future cancellation to stop the service as fast as possible.

- **Worker**: Fetcher was accidentally started too early.

    This didn't lead to any problems that we know of, but made the start a bit
    slower than it needs to.

- **Worker**: Fixed race condition where partitions were paused while fetching
  from them.

- **Worker**: Fixed theoretical race condition hang if web server started and
  stopped in quick succession.

- **Statsd**: The statsd monitor prematurely initialized the event loop
  on module import.

    We had a fix for this, but somehow forgot to remove the "hardcoded
    super" that was set to call: ``Service.__init__(self, **kwargs)``.

    The class is not even a subclass of Service anymore, and we are lucky it
    manifests merely when doing something drastic, like py.test,
    recursively importing all modules in a directory.

.. _version-1.0.12:

1.0.12
======
:release-date: 2018-06-06 1:34 P.M PDT
:release-by: Ask Solem

- **Requirements**

    + Now depends on :ref:`Mode 1.14.1 <mode:version-1.14.1>`.

- **Worker**: Producer crashing no longer causes the consumer to hang
  at shutdown while trying to publish attached messages.

.. _version-1.0.11:

1.0.11
======
:release-date: 2018-05-31  16:41 P.M PDT
:release-by: Ask Solem

- **Requirements**

    + Now depends on :ref:`Mode 1.13.0 <mode:version-1.13.0>`.

    + Now depends on :pypi:`robinhood-aiokafka`

        We have forked :pypi:`aiokafka` to fix some issues.

- Now handles missing topics automatically, so you don't have to restart
  the worker the first time when topics are missing.

- Mode now registers as a library having static type annotations.

    This conforms to :pep:`561` -- a new specification that defines
    how Python libraries register type stubs to make them available
    for use with static analyzers like :pypi:`mypy` and :pypi:`pyre-check`.

- **Typing**: Faust codebase now passes ``--strict-optional``.

- **Settings**: Added new settings

    - :setting:`broker_heartbeat_interval`
    - :setting:`broker_session_timeout`

- **Aiokafka**: Removes need for consumer partitions lock: this fixes
                rare deadlock.

- **Worker**: Worker no longer hangs for few minutes when there is an error.


.. _version-1.0.10:

1.0.10
======
:release-date: 2018-05-15  16:02 P.M PDT
:release-by: Vineet Goel

- **Worker**: Stop reading changelog when no remaining messages.

.. _version-1.0.9:

1.0.9
=====
:release-date: 2018-05-15  15:42 P.M PDT
:release-by: Vineet Goel

- **Worker**: Do not stop reading standby updates.

.. _version-1.0.8:

1.0.8
=====
:release-date: 2018-05-15 11:00 A.M PDT
:release-by: Vineet Goel

- **Tables**

    + Fixes bug due to which we were serializing ``None`` values while
      recording a key delete to the changelog. This was causing the deleted
      keys to never be deleted from the changelog.
    + We were earlier not persisting offsets of messages read during
      changelog reading (or standby recovery). This would cause longer recovery
      times if recovery was ever interrupted.

- **App**: Added flight recorder for consumer group rebalances for debugging.

.. _version-1.0.7:

1.0.7
=====
:release-date: 2018-05-14 4:53 P.M PDT
:release-by: Ask Solem

- **Requirements**

    + Now depends on :ref:`Mode 1.12.5 <mode:version-1.12.5>`.

- **App**: ``key_type`` and ``value_type`` can now be set to:

    + :class:`int`:  key/value is number stored as string
    + :class:`float`: key/value is floating point number stored as string.
    + :class:`decimal.Decimal` key/value is decimal stored as string.

- **Agent**: Fixed support for ``group_by``/``through`` after
  change to reuse the same stream after agent crashing.

- **Agent**: Fixed ``isolated_partitions=True`` after change in v1.0.3.

    Initialization of the agent-by-topic index was in :ref:`version-1.0.3`
    moved to the ``AgentManager.start`` method, but it turns out
    ``AgentManager`` is a regular class, and not a service.

    ``AgentManager`` is now a service responsible for
    starting/stopping the agents required by the app.

- **Agent**: Include active partitions in repr when
  ``isolated_partitions=True``.

- **Agent**: Removed extraneous 'agent crashed' exception in logs.

- **CLI**: Fixed autodiscovery of commands when using ``faust -A app``.

- **Consumer**: Appropriately handle closed fetcher.

- New shortcut: :func:`faust.uuid` generates UUID4 ids as string.

.. _version-1.0.6:

1.0.6
=====
:release-date: 2018-05-11 11:15 A.M PDT
:release-by: Vineet Goel

- **Requirements**:

    + Now depends on Aiokafka 0.4.7.


- **Table**: Delete keys whe raw value in changelog set to None

    This was resulting in deleted keys still being present with value None
    upon recovery.

- **Transports**: Crash app on CommitFailedError thrown by :pypi:`aiokafka`.

    App would get into a weird state upon a commit failed error thrown by the
    consumer thread in the :pypi:`aiokafka` driver.

.. _version-1.0.5:

1.0.5
=====
:release-date: 2018-05-08 4:09 P.M PDT
:release-by: Ask Solem

- **Requirements**:

    + Now depends on :ref:`Mode 1.12.4 <mode:version-1.12.4>`.

- **Agents**: Fixed problem with hanging after agent raises exception.

    If an agent raises an exception we cannot handle it within
    the stream iteration, so we need to restart the agent.

    Starting from this change, even though we restart the agent, we reuse
    the same :class:`faust.Stream` object that the crashed agent was using.

    This makes recovery more seamless and there are fewer steps
    involved.

- **Transports**: Fixed worker hanging issue introduced in 1.0.4.

    In version :ref:`version-1.0.4` we introduced a bug in the round-robin
    scheduling of topic partitions that manifested itself by hanging
    with 100% CPU usage.

    After processing all records in all topic partitions, the worker
    would spinloop.

- **API**: Added new base class for windows: :class:`faust.Window`

    There was the typing interface :class:`faust.types.windows.WindowT`,
    but now there is also a concrete base class that can be used in
    for example ``Mock(autospec=Window)``.

- **Tests**: Now takes advantage of the new
  :class:`~mode.utils.mocks.AsyncMock`.

.. _version-1.0.4:

1.0.4
=====
:release-date: 2018-05-08 11:45 A.M PDT
:release-by: Vineet Goel

- **Transports**:

    In version-1.0.2_ we implemented fair scheduling in :pypi:`aiokafka`
    transport such that while processing the worker had an equal chance of
    processing each assigned Topic. Now we also round-robin through topic
    partitions within topics such that the worker has an equal chance of
    processing message from each assigned partition within a topic as well.

.. _version-1.0.3:

1.0.3
=====
:release-date: 2018-05-07 3:45 P.M PDT
:release-by: Ask Solem

- **Tests**:

    + Adds 5650 lines of tests, increasing test coverage to 90%.

- **Requirements**:

    + Now depends on :ref:`Mode 1.12.3 <mode:version-1.12.3>`.

- **Development**:

    + CI now builds coverage.

    + CI now tests multiple CPython versions:

        * CPython 3.6.0
        * CPython 3.6.1
        * CPython 3.6.2
        * CPython 3.6.3
        * CPython 3.6.4
        * CPython 3.6.5

- **Backward incompatible changes**:

    + Removed ``faust.Set`` unused by any internal applications.

- **Fixes**:

    + ``app.agents`` did not forward app to
      :class:`~faust.agents.manager.AgentManager`.

        The agent manager does not use the app, but fixing this
        in anticipation of people writing custom agent managers.

    + :class:`~faust.agents.manager.AgentManager`: On partitions revoked
        the agent manager now makes sure there's only one call
        to each agents ``agent.on_partitions_revoked`` callback.

        This is more of a pedantic change, but could have caused problems
        for advanced topic configurations.

.. _version-1.0.2:

1.0.2
=====
:release-date: 2018-05-03 3:32 P.M PDT
:release-by: Ask Solem

- **Transports**: Implements fair scheduling in :pypi:`aiokafka` transport.

    We now round-robin through topics when processing fetched records from
    Kafka. This helps us avoid starvation when some topics have many
    more records than others, and also takes into account that different
    topics may have wildly varying partition counts.

    In this version when a worker is subscribed to partitions::

        [
            TP(topic='foo', partition=0),
            TP(topic='foo', partition=1),
            TP(topic='foo', partition=2),
            TP(topic='foo', partition=3),

            TP(topic='bar', partition=0),
            TP(topic='bar', partition=1),
            TP(topic='bar', partition=2),
            TP(topic='bar', partition=3),

            TP(topic='baz', partition=0)
        ]

    .. note::

        ``TP`` is short for *topic and partition*.

    When processing messages in these partitions, the worker will
    round robin between the topics in such a way that each topic
    will have an equal chance of being processed.

- **Transports**: Fixed crash in :pypi:`aiokafka` transport.

    The worker would attempt to commit an empty set of partitions,
    causing an exception to be raised.  This has now been fixed.

- **Stream**: Removed unused method ``Stream.tee``.

    This method was an example implementation and not used by any
    of our internal apps.

- **Stream**: Fixed bug when something raises :exc:`StopAsyncIteration`
   while processing the stream.

    The Python async iterator protocol mandates that it's illegal
    to raise :exc:`StopAsyncIteration` in an ``__aiter__`` method.

    Before this change, code such as this::

        async for value in stream:
            value = anext(other_async_iterator)

    where ``anext`` raises :exc:`StopAsyncIteration`, Python would
    have the outer ``__aiter__`` reraise that exception as::

        RuntimeError('__aiter__ raised StopAsyncIteration')

    This no longer happens as we catch the :exc:`StopAsyncIteration` exception
    early to ensure it does not propagate.

.. _version-1.0.1:

1.0.1
=====
:release-date: 2018-05-01 9:52 A.M PDT
:release-by: Ask Solem

- **Stream**: Fixed issue with using :keyword:`break` when iterating
  over stream.

    The last message in a stream would not be acked if the :keyword:`break`
    keyword was used::

        async for value in stream:
            if value == 3:
                break

- **Stream**: ``.take`` now acks events *after* buffer processed.

    Previously the events were erroneously acked at the time
    of entering the buffer.

    .. note::

        To accomplish this we maintain a list of events to ack
        as soon as the buffer is processed. The operation is
        ``O(n)`` where ``n`` is the size of the buffer, so please
        keep buffer sizes small (e.g. 1000).

        A large buffer will increase the chance of consistency
        issues where events are processed more than once.

- **Stream**: New ``noack`` modifier disables acking of messages in the
  stream.

    Use this to disable automatic acknowledgment of events::

        async for value in stream.noack():
            # manual acknowledgment
            await stream.ack(stream.current_event)

    .. admonition:: Manual Acknowledgement

        The stream is a sequence of events, where each event has a sequence
        number: the "offset".

        To mark an event as processed, so that we do not process it again,
        the Kafka broker will keep track of the last committed offset
        for any topic.

        This means "acknowledgement" works quite differently from other
        message brokers, such as RabbitMQ where you can selectively
        ack some messages, but not others.

        If the messages in the topic look like this sequence:

        .. sourcecode:: text

            1 2 3 4 5 6 7 8

        You can commit the offset for #5, only after processing all
        events before it. This means you MUST ack offsets (1, 2, 3, 4)
        *before* being allowed to commit 5 as the new offset.

- **Stream**: Fixed issue with ``.take`` not properly respecting the
  ``within`` argument.

    The new implementation of take now starts a background thread
    to fill the buffer. This avoids having to restart iterating
    over the stream, which caused issues.

.. _version-1.0.0:

1.0.0
=====
:release-date: 2018-04-27 4:13 P.M PDT
:release-by: Ask Solem

- **Models**: Raise error if ``Record.asdict()`` is overridden.

- **Models**: Can now override ``Record._prepare_dict`` to change the
  payload generated.

    For example if you want your model to serialize to a dictionary,
    but not have any fields with :const:`None` values, you can override
    ``_prepare_dict`` to accomplish this:

    .. sourcecode:: python

        class Quote(faust.Record):
            ask_price: float = None
            bid_price: float = None

            def _prepare_dict(self, data):
                # Remove keys with None values from payload.
                return {k: v for k, v in data.items() if v is not None}

        assert Quote(1.0, None).asdict() == {'ask_price': 1.0}

- **Stream**: Removed annoying ``Flight Recorder`` logging that was too noisy.
