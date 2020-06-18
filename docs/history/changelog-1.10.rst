.. _changelog-1.10:

===============================
 Change history for Faust 1.10
===============================

This document contain change notes for bugfix releases in
the Faust 1.10.x series. If you're looking for changes in the latest
series, please visit the latest :ref:`changelog`.

For even older releases you can visit the :ref:`history` section.

.. _version-1.10.4:

1.10.4
======
:release-date: 2020-02-25 3:00 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

  + Now depends on :pypi:`robinhood-aiokafka` 1.1.6

- Kafka: Fixed support for SASL authentication (Issue #468).

    Fix contributed by Julien Surloppe :github_user:`jsurloppe`.

- Adds ability to force enable block detection in production.

    To start Faust with block detection enabled in production
    add the following environment variables to the `faust worker` command:

    .. sourcecode:: bash

        F_BLOCKING_TIMEOUT='10.0'
        F_FORCE_BLOCKING_TIMEOUT=1

    The worker will now log an error if the event loop is blocked
    for more than 10 seconds.

    This is a temporary workaround until Faust version 1.11 is released,
    it will stop working in that version.
    To enable this in Faust 1.11 either use the BLOCKING_TIMEOUT environment
    variable:

    .. sourcecode:: bash

        BLOCKING_TIMEOUT='10.0'

            or use the new `blocking_timeout` setting:

        app = faust.App(..., blocking_timeout=10.0)

    Of interest is also the documentation for the new setting coming in
    1.11:

    Blocking timeout (in seconds).

    When specified the worker will start a periodic signal based
    timer that only triggers when the loop has been blocked
    for a time exceeding this timeout.

    This is the most safe way to detect blocking, but could have
    adverse effects on libraries that do not automatically
    retry interrupted system calls.

    Python itself does retry all interrupted system calls
    since version 3.5 (see :pep:`475`), but this might not
    be the case with C extensions added to the worker by the user.

    The blocking detector is a background thread
    that periodically wakes up to either arm a timer, or cancel
    an already armed timer. In pseudocode:

    .. sourcecode:: python

        while True:
            # cancel previous alarm and arm new alarm
            signal.signal(signal.SIGALRM, on_alarm)
            signal.setitimer(signal.ITIMER_REAL, blocking_timeout)
            # sleep to wakeup just before the timeout
            await asyncio.sleep(blocking_timeout * 0.96)

        def on_alarm(signum, frame):
            logger.warning('Blocking detected: ...')

    If the sleep does not wake up in time the alarm signal
    will be sent to the process and a traceback will be logged.

- Documentation improvements by:

  + Andrey Martyanov (:github_user:`martyanov`).
  + Christoph Deil (:github_user:`cdeil`).
  + Hisham Elsheshtawy (:github_user:`Sheshtawy`).
  + :github_user:`lsabi`.

.. _version-1.10.3:

1.10.3
======
:release-date: 2020-02-14 4:27 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

  + Now depends on :ref:`Mode 4.3.2 <mode:version-4.3.2>`

  + Now depends on :pypi:`robinhood-aiokafka` 1.1.5

- **Tables**: The rebalancing callback for tables now yields more
  often back to the event loop to prevent it from blocking
  the loop for too long.

- **Dist**: Removed accidental dependency on :pypi:`typing_extensions`

  Contributed by Eran Kampf (:github_user:`ekampf`).

- **Consumer**: wait empty now manually garbage collects acked
  entries from ``unacked_messages``.

    This fixes hanging during rebalance in some cases.

- **Consumer**: Wait empty now logs traceback of all running agents

    This happens when it has been waiting for agents to process
    currently waiting events, and it has been waiting for a long time.

- **Worker**: Make logged lists of partition sets more beautiful.

- **Worker**: Consolidate repeated topic names in logs to use ditto mark

- **Worker**: Make logged "setting newly assigned partitions" list
  easier to read.

    This will now:

    - Sort the numbers
    - Consolidate number ranges (e.g. ``1-5`` instead of ``1, 2, 3, 4, 5``).

- **Tables**: Recovery: Better diagnosis in logs if recovery hangs.

- **Tables**: Recovery: Show estimated time remaining until recovery is done.

.. _version-1.10.2:

1.10.2
======
:release-date: 2020-02-10 3:54 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

  + Now depends on :ref:`Mode 4.3.1 <mode:version-4.3.0>`.

  + Now depends on :pypi:`robinhood-aiokafka` 1.1.4

- Aiokafka: Livelock and stream timeouts replaced with better instrumentation.

    This will let us better isolate the cause of
    a worker that is not progressing. The problem could originate in
    code written by the user, the :pypi:`aiokafka` Kafka client, or a
    core component of the Faust worker could be malfunctioning.

    To help diagnose the cause of such disruption, the worker now logs when

    1) :pypi:`aiokafka` stops sending fetch requests.
    2) Kafka stops responding to fetch requests.
    3) :pypi:`aiokafka` stops updating highwater offset.
    4) a stream stops processing events, or is processing very slowly.
    5) the worker stops committing offsets, or the time it takes to complete
       the commit operation is exorbitant.

.. _version-1.10.1:

1.10.1
======
:release-date: 2020-01-22 5:00 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

  + Now depends on :ref:`Mode 4.3.0 <mode:version-4.3.0>`.

- Consumer: Default for the :setting:`consumer_max_fetch_size` setting
  is now 1MB.

    Make sure to consider the total number of partitions a worker node
    can be assigned when tweaking this value.

    If an app is subscribing to 4 topics, that have 100 partitions
    each, and only a single worker is running, this will mean
    the maximum fetch size at this point is 4 * 100MB.

    When the worker is rebalancing it needs to flush any current
    fetch requests before continuing, and if that much data is left
    in the socket buffer it can cause another rebalance to happen,
    then another, then another, ending up in a rebalancing loop.

- Worker: Fixed problem of timers waking up too late.

    Turns out some parts of the worker were blocking the event loop
    causing timers to wake up too late.

    We have found a way to identify such blocking and have
    added some carefully placed ``asyncio.sleep(0)`` statements
    to minimize blocking.

- Worker: Emit more beautiful logs by converting lists of topic partitions
  to ANSI tables.

- Stream: Fixed race condition where stopping a stream twice would
  cause it to wait indefinitely.

- Tables: Fixes hang at startup when using global table (Issue #507)

- Agents: Fixed RPC hanging in clients (Issue #509).

    Contributed by Jonathan A. Booth (:github_user:`jbooth-mastery`).

.. _version-1.10.0:

1.10.0
======
:release-date: 2020-01-13 11:32 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

    + Now depends on :pypi:`robinhood-aiokafka` 1.1.3

    + Now depends on :ref:`Mode 4.1.9 <mode:version-4.1.9>`.


.. _v1_10-news:

News
----

- Agents: ``use_reply_headers`` is now enabled by default (Issue #469).

    This affects users of ``Agent.ask``, ``.cast``, ``.map``, ``.kvmap``,
    and ``.join`` only.

    This requires a Kafka broker with headers support. If you want
    to avoid making this change you can disable it manually
    by passing the ``use_reply_headers`` argument to the agent decorator:

    .. sourcecode:: python

        @app.agent(use_reply_headers=False)

- Models: Support fields with arbitrarily nested type expressions.

    This extends model fields to support arbitrarily nested type
    expressions, such as ``List[Dict[str, List[Set[MyModel]]]``

- Models: Support for fields that have named tuples.

    This includes named tuples with fields that are also models.

    For example:

    .. sourcecode:: python

        from typing import NamedTuple
        from faust import Record

        class Point(Record):
            x: int
            y: int

        class NamedPoint(NamedTuple):
            name: str
            point: Point

        class Arena(Record):
            points: List[NamedPoint]

    Note that this does not currently support ``collections.namedtuple``.

- Models: Support for fields that are unions of models,
    such as ``Union[ModelX, ModelY]``.

- Models: Optimizations and backward incompatible changes.

    + Serialization is now 4x faster.
    + Deserialization is 2x faster.

    Related fields are now lazily loaded, so models and complex structures
    are only loaded as needed.

    One important change is that serializing a model will
    no longer traverse the structure for child models, instead we rely
    on the json serializer to call `Model.__json__()` during serializing.

    Specifically this means, where previously having models

    .. sourcecode:: python

        class X(Model):
            name: str

        class Y(Model):
            x: X

    and calling ``Y(X('foo')).to_representation()`` it would return:

    .. sourcecode:: pycon

        >>> Y(X('foo')).to_representation()
        {
            'x': {
                'name': 'foo',
                '__faust': {'ns': 'myapp.X'},
            },
            '__faust': {'ns': 'myapp.Y'},
        }

    after this change it will instead return the objects as-is:

    .. sourcecode:: pycon

        >>> Y(X('foo')).to_representation()
        {
            'x': X(name='foo'),
            '__faust': {'ns': 'myapp.Y'},
        }

    This is a backward incompatible change for anything that relies
    on the previous behavior, but in most apps will be fine as the
    Faust json serializer will automatically handle models and call
    ``Model.__json__()`` on them as needed.

    **Removed attributes**

    The following attributes have been removed from ``Model._options``,
    and :class:`~faust.types.FieldDescriptorT`, as they are no longer needed,
    or no longer make sense when supporting arbitrarily nested structures.

    *:class:`Model._options <faust.types.models.ModelOptions>`*

    - ``.models``

        Previously map of fields that have related models.
        This index is no longer used, and a field can have multiple
        related models now.  You can generate this index using the
        statement:

        .. sourcecode:: python

            {field: field.related_models
                for field in model._options.descriptors
                if field.related_models}

    - ``.modelattrs``

    - ``.field_coerce``

    - ``.initfield``

    - ``.polyindex``

    *:class:`~faust.types.FieldDescriptorT`*

    - ``generic_type``
    - ``member_type``

- Tables: Fixed behavior of global tables.

    Contributed by DhruvaPatil98 (:github_user:`DhruvaPatil98`).

- Tables: Added ability to iterate through all keys in a global table.

    Contributed by DhruvaPatil98 (:github_user:`DhruvaPatil98`).

- Tables: Attempting to call ``keys()``/``items()``/``values()`` on
  a windowset now raises an exception.

    This change was added to avoid unexpected behavior.

    Contributed by Sergej Herbert (:github_user:`fr-ser`).

- Models: Added new bool field type :class:`~faust.models.fields.BooleanField`.

    Thanks to John Heinnickel.

- aiokafka: Now raises an exception when topic name length exceeds 249
  characters (Issue #411).

- New :setting:`broker_api_version` setting.

    The new setting acts as default for both the new
    :setting:`consumer_api_version` setting, and the previously existing
    :setting:`broker_api_version` setting.

    This means you can now configure the API version for everything
    by setting the :setting:`broker_api_version` setting, while still
    being able to configure the API version individually for producers
    and consumers.

- New :setting:`consumer_api_version` setting.

    See above.

- New :setting:`broker_rebalance_timeout` setting.

- Test improvements

    Contributed by Marcos Schroh (:github_user:`marcosschroh`).

- Documentation improvements by:

    - Bryant Biggs (:github_user:`bryantbiggs`).
    - Christoph Deil (:github_user:`cdeil`).
    - Tim Gates (:github_user:`timgates42`).
    - Marcos Schroh (:github_user:`marcosschroh`).

Fixes
-----

- Consumer: Properly wait for all agents and the table manager to
  start and subscribe to topics before sending subscription list to Kafka.
  (Issue #501).

    This fixes a race condition where the subscription list is sent
    before all agents have started subscribing to the topics they need.
    At worst this result ended in a crash at startup (set
    size changed during iteration).

    Contributed by DhruvaPatil98 (:github_user:`DhruvaPatil98`).

- Agents: Fixed ``Agent.test_context()`` sink support (Issue #495).

    Fix contributed by Denis Kovalev (:github_user:`aikikode`).

- aiokafka: Fixes crash in ``on_span_cancelled_early`` when tracing disabled.

