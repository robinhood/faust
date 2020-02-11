.. _guide-settings:

====================================
 Configuration Reference
====================================

.. _settings-required:

Required Settings
=================

.. setting:: id

``id``
------

:type: ``str``

A string uniquely identifying the app, shared across all
instances such that two app instances with the same `id` are
considered to be in the same "group".

This parameter is required.

.. admonition:: The id and Kafka

    When using Kafka, the id is used to generate app-local topics, and
    names for consumer groups.

.. _settings-common:

Commonly Used Settings
======================

.. setting:: debug

``debug``
---------

:type: :class:`bool`
:default: :const:`False`

Use in development to expose sensor information endpoint.


.. tip::

    If you want to enable the sensor statistics endpoint in production,
    without enabling the :setting:`debug` setting, you can do so
    by adding the following code:

    .. sourcecode:: python

        app.web.blueprints.add('/stats/', 'faust.web.apps.stats:blueprint')


.. setting:: broker

``broker``
----------

:type: ``Union[str, URL, List[URL]]``
:default: ``[URL("kafka://localhost:9092")]``

Faust needs the URL of a "transport" to send and receive messages.

Currently, the only supported production transport is ``kafka://``.
This uses the :pypi:`aiokafka` client under the hood, for consuming and
producing messages.

You can specify multiple hosts at the same time by separating them using
the semi-comma:

.. sourcecode:: text

    kafka://kafka1.example.com:9092;kafka2.example.com:9092

Which in actual code looks like this:

.. sourcecode:: python

    app = faust.App(
        'id',
        broker='kafka://kafka1.example.com:9092;kafka2.example.com:9092',
    )

You can also pass a list of URLs:

.. sourcecode:: python

    app = faust.App(
        'id',
        broker=['kafka://kafka1.example.com:9092',
                'kafka://kafka2.example.com:9092'],
    )

.. seealso::

    You can configure the transport used for consuming and producing
    separately, by setting the :setting:`broker_consumer` and
    :setting:`broker_producer` settings.

    This setting is used as the default.

Available Transports
~~~~~~~~~~~~~~~~~~~~

- ``kafka://``

    Alias to ``aiokafka://``

- ``aiokafka://``

    The recommended transport using the :pypi:`aiokafka` client.

    Limitations: None

- ``confluent://``

    Experimental transport using the :pypi:`confluent-kafka` client.

    Limitations: Does not do sticky partition assignment (not
        suitable for tables), and do not create any necessary internal
        topics (you have to create them manually).

.. setting:: broker_credentials

``broker_credentials``
----------------------

.. versionadded:: 1.5

:type: :class:`~faust.types.auth.CredentialsT`
:default: :const:`None`

Specify the authentication mechanism to use when connecting to the
broker.

The default is to not use any authentication.

.. _auth-sasl:

SASL Authentication
~~~~~~~~~~~~~~~~~~~

You can enable SASL authentication via plain text:

.. sourcecode:: python

    app = faust.App(
        broker_credentials=faust.SASLCredentials(
            username='x',
            password='y',
        ))

.. warning::

    Do not use literal strings when specifying passwords in production,
    as they can remain visible in stack traces.

    Instead the best practice is to get the password from a configuration
    file, or from the environment:

    .. sourcecode:: python

        BROKER_USERNAME = os.environ.get('BROKER_USERNAME')
        BROKER_PASSWORD = os.environ.get('BROKER_PASSWORD')

        app = faust.App(
            broker_credentials=faust.SASLCredentials(
                username=BROKER_USERNAME,
                password=BROKER_PASSWORD,
            ))

.. _auth-gssapi:

GSSAPI Authentication
~~~~~~~~~~~~~~~~~~~~~

GSSAPI authentication over plain text:

.. sourcecode:: python

    app = faust.App(
        broker_credentials=faust.GSSAPICredentials(
            kerberos_service_name='faust',
            kerberos_domain_name='example.com',
        ),
    )

GSSAPI authentication over SSL:

.. sourcecode:: python

    import ssl
    ssl_context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH, cafile='ca.pem')
    ssl_context.load_cert_chain('client.cert', keyfile='client.key')

    app = faust.App(
        broker_credentials=faust.GSSAPICredentials(
            kerberos_service_name='faust',
            kerberos_domain_name='example.com',
            ssl_context=ssl_context,
        ),
    )

.. _auth-ssl:

SSL Authentication
~~~~~~~~~~~~~~~~~~

Provide an SSL context for the Kafka broker connections.

This allows Faust to use a secure SSL/TLS connection for the Kafka connections
and enabling certificate-based authentication.

.. sourcecode:: python

    import ssl

    ssl_context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH, cafile='ca.pem')
    ssl_context.load_cert_chain('client.cert', keyfile='client.key')
    app = faust.App(..., broker_credentials=ssl_context)

.. setting:: store

``store``
---------

:type: ``str``
:default: ``URL("memory://")``

The backend used for table storage.

Tables are stored in-memory by default, but you should
not use the ``memory://`` store in production.

In production, a persistent table store, such as ``rocksdb://`` is
preferred.

.. setting:: cache

``cache``
---------

.. versionadded:: 1.2

:type: ``str``
:default: ``URL("memory://")``

Optional backend used for Memcached-style caching.
URL can be: ``redis://host``, ``rediscluster://host``, or ``memory://``.

.. setting:: processing_guarantee

``processing_guarantee``
------------------------

.. versionadded:: 1.5

:type: ``str``
:default: ``"at_least_once"``

The processing guarantee that should be used.

Possible values are "at_least_once" (default) and "exactly_once".
Note that if exactly-once processing is enabled consumers are configured with
``isolation.level="read_committed"`` and producers are configured with
``retries=Integer.MAX_VALUE`` and ``enable.idempotence=true`` per default.
Note that by default exactly-once processing requires a cluster of at least
three brokers what is the recommended setting for production.
For development you can change this, by adjusting broker setting
``transaction.state.log.replication.factor`` to the number of brokers you want to use.

.. setting:: autodiscover

``autodiscover``
----------------

:type: ``Union[bool, Iterable[str], Callable[[], Iterable[str]]]``

Enable autodiscovery of agent, task, timer, page and command decorators.

Faust has an API to add different :mod:`asyncio` services and other user
extensions, such as "Agents", HTTP web views, command-line commands, and
timers to your Faust workers.  These can be defined in any module, so to
discover them at startup, the worker needs to traverse packages looking
for them.

.. warning::

    The autodiscovery functionality uses the :pypi:`Venusian` library to
    scan wanted packages for ``@app.agent``, ``@app.page``,
    ``@app.command``, ``@app.task`` and ``@app.timer`` decorators,
    but to do so, it's required to traverse the package path and import
    every module in it.

    Importing random modules like this can be dangerous so make sure you
    follow Python programming best practices. Do not start
    threads; perform network I/O; do test monkey-patching for mocks or similar,
    as a side effect of importing a module.  If you encounter a case such as
    this then please find a way to perform your action in a lazy manner.

.. warning::

    If the above warning is something you cannot fix, or if it's out of your
    control, then please set ``autodiscover=False`` and make sure the worker
    imports all modules where your decorators are defined.

The value for this argument can be:

``bool``
    If ``App(autodiscover=True)`` is set, the autodiscovery will
    scan the package name described in the ``origin`` attribute.

    The ``origin`` attribute is automatically set when you start
    a worker using the :program:`faust` command line program, for example:

    .. sourcecode:: console

        faust -A example.simple worker

    The :option:`-A <faust -A>`, option specifies the app, but you can also
    create a shortcut entry point by calling ``app.main()``:

    .. sourcecode:: python

        if __name__ == '__main__':
            app.main()

    Then you can start the :program:`faust` program by executing for example
    ``python myscript.py worker --loglevel=INFO``, and it will use the correct
    application.

``Sequence[str]``
    The argument can also be a list of packages to scan::

        app = App(..., autodiscover=['proj_orders', 'proj_accounts'])

``Callable[[], Sequence[str]]``
    The argument can also be a function returning a list of packages
    to scan::

        def get_all_packages_to_scan():
            return ['proj_orders', 'proj_accounts']

        app = App(..., autodiscover=get_all_packages_to_scan)

False)

    If everything you need is in a self-contained module, or you import the
    stuff you need manually, just set ``autodiscover`` to False and don't
    worry about it :-)

.. admonition:: Django

    When using :pypi:`Django` and the :envvar:`DJANGO_SETTINGS_MODULE`
    environment variable is set, the Faust app will scan all packages found
    in the ``INSTALLED_APPS`` setting.

    If you're using Django you can use this to scan for
    agents/pages/commands in all packages defined in ``INSTALLED_APPS``.

    Faust will automatically detect that you're using Django and do the
    right thing if you do::

        app = App(..., autodiscover=True)

    It will find agents and other decorators in all of the reusable Django
    applications. If you want to manually control what packages are
    traversed, then provide
    a list::

        app = App(..., autodiscover=['package1', 'package2'])

    or if you want exactly :const:`None` packages to be traversed, then
    provide a False:

        app = App(.., autodiscover=False)

    which is the default, so you can simply omit the argument.

.. tip::

    For manual control over autodiscovery, you can also call the
    :meth:`@discover` method manually.

.. setting:: version

``version``
-----------

:type: :class:`int`
:default: 1

Version of the app, that when changed will create a new isolated
instance of the application. The first version is 1, the second version is 2,
and so on.

.. admonition:: Source topics will not be affected by a version change.

    Faust applications will use two kinds of topics: source topics, and
    internally managed topics. The source topics are declared by the producer,
    and we do not have the opportunity to modify any configuration settings,
    like number of partitions for a source topic; we may only consume from
    them. To mark a topic as internal, use: ``app.topic(..., internal=True)``.

.. setting:: timezone

``timezone``
------------

:type: :class:`datetime.tzinfo`
:default: :class:`datetime.timezone.utc`

The timezone used for date-related functionality such as cronjobs.

.. versionadded:: 1.4

.. setting:: datadir

``datadir``
-----------

:type: ``Union[str, pathlib.Path]``
:default: ``Path(f"{app.conf.id}-data")``
:environment: :envvar:`FAUST_DATADIR`, :envvar:`F_DATADIR`

The directory in which this instance stores the data used by local tables, etc.

.. seealso::

    - The data directory can also be set using the :option:`faust --datadir`
      option, from the command-line, so there's usually no reason to provide
      a default value when creating the app.

.. setting:: tabledir

``tabledir``
------------

:type: ``Union[str, pathlib.Path]``
:default: ``"tables"``

The directory in which this instance stores local table data.
Usually you will want to configure the :setting:`datadir` setting, but if you
want to store tables separately you can configure this one.

If the path provided is relative (it has no leading slash), then the path will
be considered to be relative to the :setting:`datadir` setting.

.. setting:: id_format

``id_format``
-------------

:type: :class:`str`
:default: ``"{id}-v{self.version}"``

The format string used to generate the final :setting:`id` value by combining
it with the :setting:`version` parameter.

.. setting:: logging_config

``logging_config``
------------------

.. versionadded:: 1.5.0

Optional dictionary for logging configuration, as supported
by :func:`logging.config.dictConfig`.

.. setting:: loghandlers

``loghandlers``
---------------

:type: ``List[logging.LogHandler]``
:default: ``[]``

Specify a list of custom log handlers to use in worker instances.

.. setting:: origin

``origin``
----------

:type: :class:`str`
:default: :const:`None`

The reverse path used to find the app, for example if the app is located in::

    from myproj.app import app

Then the ``origin`` should be ``"myproj.app"``.

The :program:`faust worker` program will try to automatically set the origin,
but if you are having problems with auto generated names then you can set
origin manually.


.. _settings-serialization:

Serialization Settings
======================

.. setting:: key_serializer

``key_serializer``
------------------

:type: ``Union[str, Codec]``
:default: ``"raw"``

Serializer used for keys by default when no serializer is specified, or a
model is not being used.

This can be the name of a serializer/codec, or an actual
:class:`faust.serializers.codecs.Codec` instance.

.. seealso::

    - The :ref:`codecs` section in the model guide -- for more information
      about codecs.

.. setting:: value_serializer

``value_serializer``
--------------------

:type: ``Union[str, Codec]``
:default: ``"json"``

Serializer used for values by default when no serializer is specified, or a
model is not being used.

This can be string, the name of a serializer/codec, or an actual
:class:`faust.serializers.codecs.Codec` instance.

.. seealso::

    - The :ref:`codecs` section in the model guide -- for more information
      about codecs.

.. _settings-topic:

Topic Settings
==============

.. setting:: topic_replication_factor

``topic_replication_factor``
----------------------------

:type: :class:`int`
:default: ``1``

The default replication factor for topics created by the application.

.. note::

    Generally this should be the same as the configured
    replication factor for your Kafka cluster.

.. setting:: topic_partitions

``topic_partitions``
--------------------

:type: :class:`int`
:default: ``8``

Default number of partitions for new topics.

.. note::

    This defines the maximum number of workers we could distribute the
    workload of the application (also sometimes referred as the sharding
    factor of the application).

.. setting:: topic_allow_declare

``topic_allow_declare``
-----------------------

.. versionadded:: 1.5

:type: :class:`bool`
:default: :const:`True`

This setting disables the creation of internal topics.

Faust will only create topics that it considers to be fully owned and managed,
such as intermediate repartition topics, table changelog topics etc.

Some Kafka managers does not allow services to create topics, in that case
you should set this to :const:`False`.

.. setting:: topic_disable_leader

``topic_disable_leader``
------------------------

:type: :class:`bool`
:default: :const:`False`

This setting disables the creation of the leader election topic.

If you're not using the ``on_leader=True`` argument to task/timer/etc.,
decorators then use this setting to disable creation of the topic.

.. _settings-broker:

Advanced Broker Settings
========================

.. setting:: broker_consumer

``broker_consumer``
-------------------

:type: ``Union[str, URL, List[URL]]``
:default: :const:`None`

You can use this setting to configure the transport used for
producing and consuming separately.

If not set the value found in :setting:`broker` will be used.

.. setting:: broker_producer

``broker_producer``
-------------------

:type: ``Union[str, URL, List[URL]]``
:default: :const:`None`

You can use this setting to configure the transport used for
producing and consuming separately.

If not set the value found in :setting:`broker` will be used.

.. setting:: broker_api_version

``broker_api_version``
----------------------

.. versionadded:: 1.10

:type: ``str``
:default: ``"auto"``

This setting is also the default for :setting:`consumer_api_version`, and
:setting:`producer_api_version`.

Negotiate producer protocol version.

The default value - "auto" means use the latest version supported by both
client and server.

Any other version set means you are requesting a specific version of the
protocol.

Example Kafka uses:

Disable sending headers for all messages produced
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Kafka headers support was added in Kafka 0.11, so you can specify
``broker_api_version="0.10"`` to remove the headers from messages.

.. setting:: broker_client_id

``broker_client_id``
--------------------

:type: ``str``
:default: ``f"faust-{VERSION}"``

There is rarely any reason to configure this setting.

The client id is used to identify the software used, and is not usually
configured by the user.

.. setting:: broker_request_timeout

``broker_request_timeout``
--------------------------

.. versionadded:: 1.4.0

:type: :class:`int`
:default: ``90.0`` (seconds)

Kafka client request timeout.

.. note::

    The request timeout must not be less than the
    :setting:`broker_session_timeout`.

.. setting:: broker_commit_every

``broker_commit_every``
-----------------------

:type: :class:`int`
:default: ``10_000``

Commit offset every n messages.

See also :setting:`broker_commit_interval`, which is how frequently
we commit on a timer when there are few messages being received.

.. setting:: broker_commit_interval

``broker_commit_interval``
--------------------------

:type: :class:`float`, :class:`~datetime.timedelta`
:default: ``2.8``

How often we commit messages that have been fully processed (:term:`acked`).

.. setting:: broker_commit_livelock_soft_timeout

``broker_commit_livelock_soft_timeout``
---------------------------------------

:type: :class:`float`, :class:`~datetime.timedelta`
:default: ``300.0`` (five minutes)

How long time it takes before we warn that the Kafka commit offset has
not advanced (only when processing messages).

.. setting:: broker_check_crcs

``broker_check_crcs``
---------------------

:type: :class:`bool`
:default: :const:`True`

Automatically check the CRC32 of the records consumed.

.. setting:: broker_heartbeat_interval

``broker_heartbeat_interval``
-----------------------------

.. versionadded:: 1.0.11

:type: :class:`int`
:default: ``3.0`` (three seconds)

How often we send heartbeats to the broker, and also how often
we expect to receive heartbeats from the broker.

If any of these time out, you should increase this setting.

.. setting:: broker_session_timeout

``broker_session_timeout``
--------------------------

.. versionadded:: 1.0.11

:type: :class:`int`
:default: ``60.0`` (one minute)

How long to wait for a node to finish rebalancing before the broker
will consider it dysfunctional and remove it from the cluster.

Increase this if you experience the cluster being in a state of constantly
rebalancing, but make sure you also increase the
:setting:`broker_heartbeat_interval` at the same time.

.. note::

    The session timeout must not be greater than the
    :setting:`broker_request_timeout`.


.. setting:: broker_rebalance_timeout

``broker_rebalance_timeout``
----------------------------

.. versionadded:: 1.10

:type: :class:`int`
:default: ``60.0`` (one minute)

How long to wait for a node to finish rebalancing before the broker
will consider it dysfunctional and remove it from the cluster.

Increase this if you experience the cluster being in a state of constantly
rebalancing, but make sure you also increase the
:setting:`broker_heartbeat_interval` at the same time.

.. note::

    The session timeout must not be greater than the
    :setting:`broker_request_timeout`.

.. setting:: broker_max_poll_records

``broker_max_poll_records``
---------------------------

.. versionadded:: 1.4

:type: :class:`int`
:default: :const:`None`

The maximum number of records returned in a single call to poll().
If you find that your application needs more time to process messages
you may want to adjust :setting:`broker_max_poll_records` to tune the
number of records that must be handled on every loop iteration.

.. setting:: broker_max_poll_interval

``broker_max_poll_interval``
----------------------------

.. versionadded:: 1.7

:type: :class:`float`
:default: ``1000.0``

The maximum allowed time (in seconds) between calls to consume messages
If this interval is exceeded the consumer
is considered failed and the group will rebalance in order to reassign
the partitions to another consumer group member. If API methods block
waiting for messages, that time does not count against this timeout.

See `KIP-62`_ for technical details.

.. _`KIP-62`:
    https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread

.. _settings-consumer:

Advanced Consumer Settings
==========================

.. setting:: consumer_api_version

``consumer_api_version``
------------------------

.. versionadded:: 1.10

:type: :class:`str`
:default: :setting:`broker_api_version`

Configures the broker API version to use for consumers.
See :setting:`broker_api_version` for more information.

.. setting:: consumer_max_fetch_size

``consumer_max_fetch_size``
---------------------------

.. versionadded:: 1.4

:type: :class:`int`
:default: ``1048576`` (one megabyte per partition).

The maximum amount of data per-partition the server will return. This size
must be at least as large as the maximum message size.

Note: This is PER PARTITION, so a limit of 1Mb when your
workers consume from 10 topics having 100 partitions each,
means a fetch request can be up to a gigabyte (10 * 100 * 1Mb),
This limit being too generous may cause
rebalancing issues: if the amount of time required
to flush pending data stuck in socket buffers exceed
the rebalancing timeout.

You must keep this limit low enough to account
for many partitions being assigned to a single node.

.. setting:: consumer_auto_offset_reset

``consumer_auto_offset_reset``
------------------------------

.. versionadded:: 1.5

:type: :class:`string`
:default: ``"earliest"``

Where the consumer should start reading messages from when there is no initial
offset, or the stored offset no longer exists, e.g. when starting a new
consumer for the first time. Options include 'earliest', 'latest', 'none'.

.. setting:: ConsumerScheduler

``ConsumerScheduler``
---------------------

.. versionadded:: 1.5

:type: ``Union[str, Type[SchedulingStrategyT]``
:default: ``faust.transport.utils.DefaultSchedulingStrategy``

A strategy which dictates the priority of topics and partitions
for incoming records.
The default strategy does first round-robin over topics and then
round-robin over partitions.

Example using a class::

    class MySchedulingStrategy(DefaultSchedulingStrategy):
        ...

    app = App(..., ConsumerScheduler=MySchedulingStrategy)

Example using the string path to a class::

    app = App(..., ConsumerScheduler='myproj.MySchedulingStrategy')

.. _settings-producer:

Advanced Producer Settings
==========================

.. setting:: producer_compression_type

``producer_compression_type``
-----------------------------

:type: :class:`string`
:default: ``None``

The compression type for all data generated by the producer. Valid values are
`gzip`, `snappy`, `lz4`, or :const:`None`.

.. setting:: producer_linger_ms

``producer_linger_ms``
-----------------------------

:type: :class:`int`
:default: ``0``

Minimum time to batch before sending out messages from the producer.

Should rarely have to change this.

.. setting:: producer_max_batch_size

``producer_max_batch_size``
---------------------------

:type: :class:`int`
:default: 16384

Max size of each producer batch, in bytes.

.. setting:: producer_max_request_size

``producer_max_request_size``
-----------------------------

:type: :class:`int`
:default: ``1000000``

Maximum size of a request in bytes in the producer.

Should rarely have to change this.

.. setting:: producer_acks

``producer_acks``
-----------------------------

:type: :class:`int`
:default: ``-1``

The number of acknowledgments the producer requires the leader to have
received before considering a request complete. This controls the
durability of records that are sent. The following settings are common:

* ``0``: Producer will not wait for any acknowledgment from the server at all.
  The message will immediately be considered sent. (Not recommended)
* ``1``: The broker leader will write the record to its local log but will
  respond without awaiting full acknowledgment from all followers. In this
  case should the leader fail immediately after acknowledging the record but
  before the followers have replicated it then the record will be lost.
* ``-1``: The broker leader will wait for the full set of in-sync replicas to
  acknowledge the record. This guarantees that the record will not be lost as
  long as at least one in-sync replica remains alive. This is the strongest
  available guarantee.

.. setting:: producer_request_timeout

``producer_request_timeout``
----------------------------

.. versionadded:: 1.4

:type: :class:`float`, :class:`datetime.timedelta`
:default: ``1200.0`` (20 minutes)

Timeout for producer operations.
This is set high by default, as this is also the time when producer batches
expire and will no longer be retried.

.. setting:: producer_api_version

``producer_api_version``
------------------------

.. versionadded:: 1.5.3

:type: :class:`str`
:default: :setting:`broker_api_version`

Configures the broker API version to use for producers.
See :setting:`broker_api_version` for more information.

.. setting:: producer_partitioner

``producer_partitioner``
------------------------

.. versionadded:: 1.2

:type: ``Callable[[bytes, List[int], List[int]], int]``
:default: :const:`None`

The Kafka producer can be configured with a custom partitioner
to change how keys are partitioned when producing to topics.

The default partitioner for Kafka is implemented as follows,
and can be used as a template for your own partitioner:

.. sourcecode:: python

    import random
    from typing import List
    from kafka.partitioner.hashed import murmur2

    def partition(key: bytes,
                  all_partitions: List[int],
                  available: List[int]) -> int:
        """Default partitioner.

        Hashes key to partition using murmur2 hashing (from java client)
        If key is None, selects partition randomly from available,
        or from all partitions if none are currently available

        Arguments:
            key: partitioning key
            all_partitions: list of all partitions sorted by partition ID.
            available: list of available partitions in no particular order
        Returns:
            int: one of the values from ``all_partitions`` or ``available``.
        """
        if key is None:
            source = available if available else all_paritions
            return random.choice(source)
        index: int = murmur2(key)
        index &= 0x7fffffff
        index %= len(all_partitions)
        return all_partitions[index]


.. _settings-table:

Advanced Table Settings
=======================

.. setting:: table_cleanup_interval

``table_cleanup_interval``
--------------------------

:type: :class:`float`, :class:`~datetime.timedelta`
:default: ``30.0``

How often we cleanup tables to remove expired entries.

.. setting:: table_standby_replicas

``table_standby_replicas``
--------------------------

:type: :class:`int`
:default: ``1``

The number of standby replicas for each table.

.. setting:: table_key_index_size

``table_key_index_size``
------------------------

.. versionadded:: 1.8

:type: :class:`int`
:default: ``1000``

Tables keep a cache of key to partition number to speed up
table lookups.

This setting configures the maximum size of that cache.

.. _settings-stream:

Advanced Stream Settings
========================

.. setting:: stream_buffer_maxsize

``stream_buffer_maxsize``
-------------------------

:type: :class:`int`
:default: 4096

This setting control back pressure to streams and agents reading from streams.

If set to 4096 (default) this means that an agent can only keep at most
4096 unprocessed items in the stream buffer.

Essentially this will limit the number of messages a stream can "prefetch".

Higher numbers gives better throughput, but do note that if your agent
sends messages or update tables (which sends changelog messages).

This means that if the buffer size is large, the
:setting:`broker_commit_interval` or :setting:`broker_commit_every` settings
must be set to commit frequently, avoiding back pressure from building up.

A buffer size of 131_072 may let you process over 30,000 events a second
as a baseline, but be careful with a buffer size that large when you also
send messages or update tables.

.. setting:: stream_recovery_delay

``stream_recovery_delay``
-------------------------
:type: ``Union[float, datetime.timedelta]``
:default: ``0.0``

Number of seconds to sleep before continuing after rebalance.
We wait for a bit to allow for more nodes to join/leave before
starting recovery tables and then processing streams. This to minimize
the chance of errors rebalancing loops.

.. versionchanged:: 1.5.3

    Disabled by default.

.. setting:: stream_wait_empty

``stream_wait_empty``
---------------------

:type: :class:`bool`
:default: :const:`True`

This setting controls whether the worker should wait for the currently
processing task in an agent to complete before rebalancing or shutting down.

On rebalance/shut down we clear the stream buffers. Those events will be
reprocessed after the rebalance anyway, but we may have already started
processing one event in every agent, and if we rebalance we will process
that event again.

By default we will wait for the currently active tasks, but if your
streams are idempotent you can disable it using this setting.

.. setting:: stream_processing_timeout

``stream_processing_timeout``
-----------------------------

:type: ``Union[float, datetime.timedelta]``
:default: ``300.0`` (5 minutes)

Timeout (in seconds) for processing events in the stream.
If processing of a single event exceeds this time we log an error,
but do not stop processing.

If you are seeing a warning like this you should either

1) increase this timeout to allow agents to spend more time
   on a single event, or
2) add a timeout to the operation in the agent, so stream processing
   always completes before the timeout.

The latter is preferred for network operations such as web requests.
If a network service you depend on is temporarily offline you should
consider doing retries (send to separate topic):

.. sourcecode:: python

    main_topic = app.topic('main')
    deadletter_topic = app.topic('main_deadletter')

    async def send_request(value, timeout: float = None) -> None:
        await app.http_client.get('http://foo.com', timeout=timeout)

    @app.agent(main_topic)
    async def main(stream):
        async for value in stream:
        try:
            await send_request(value, timeout=5)
        except asyncio.TimeoutError:
            await deadletter_topic.send(value)

    @app.agent(deadletter_topic)
        async def main_deadletter(stream):
            async for value in stream:
            # wait for 30 seconds before retrying.
            await stream.sleep(30)
            await send_request(value)

.. setting:: stream_publish_on_commit

``stream_publish_on_commit``
----------------------------
:type: :class:`bool`
:default: :const:`False`

If enabled we buffer up sending messages until the
source topic offset related to that processing is committed.
This means when we do commit, we may have buffered up a LOT of messages
so commit needs to happen frequently (make sure to decrease
:setting:`broker_commit_every`).

.. _settings-worker:

Advanced Worker Settings
========================

.. setting:: worker_redirect_stdouts

``worker_redirect_stdouts``
---------------------------

:type: :class:`bool`
:default: :const:`True`

Enable to have the worker redirect output to :data:`sys.stdout` and
:data:`sys.stderr` to the Python logging system.

Enabled by default.

.. setting:: worker_redirect_stdouts_level

``worker_redirect_stdouts_level``
---------------------------------

:type: :class:`str`/:class:`int`
:default: ``"WARN"``

The logging level to use when redirect STDOUT/STDERR to logging.

.. _settings-web:

Advanced Web Server Settings
============================

.. setting:: web

``web``
-------

.. versionadded:: 1.2

:type: :class:`str`
:default: ``URL("aiohttp://")``

The web driver to use.

.. setting:: web_enabled

``web_enabled``
---------------

.. versionadded:: 1.2

:type: :class:`bool`
:default: :const:`True`

Enable web server and other web components.

This option can also be set using :option:`faust worker --without-web`.

.. setting:: web_transport

``web_transport``
-----------------

.. versionadded:: 1.2

:type: :class:`str`
:default: ``URL("tcp://")``

The network transport used for the web server.

Default is to use TCP, but this setting also enables you to use
Unix domain sockets.  To use domain sockets specify an URL including
the path to the file you want to create like this:

.. sourcecode:: text

    unix:///tmp/server.sock

This will create a new domain socket available in :file:`/tmp/server.sock`.

.. setting:: canonical_url

``canonical_url``
-----------------

:type:  :class:`str`
:default: ``URL(f"http://{web_host}:{web_port}")``

You shouldn't have to set this manually.

The canonical URL defines how to reach the web server on a running
worker node, and is usually set by combining the :option:`faust worker --web-host`
and :option:`faust worker --web-port` command line arguments, not
by passing it as a keyword argument to :class:`App`.

.. setting:: web_host

``web_host``
------------

.. versionadded:: 1.2

:type: :class:`str`
:default: ``f"{socket.gethostname()}"``

Hostname used to access this web server, used for generating
the :setting:`canonical_url` setting.

This option is usually set by :option:`faust worker --web-host`,
not by passing it as a keyword argument to :class:`app`.

.. setting:: web_port

``web_port``
------------

.. versionadded:: 1.2

:type: :class:`int`
:default: ``6066``

A port number between 1024 and 65535 to use for the web server.

This option is usually set by :option:`faust worker --web-port`,
not by passing it as a keyword argument to :class:`app`.

.. setting:: web_bind

``web_bind``
------------

.. versionadded:: 1.2

:type: :class:`str`
:default: ``"0.0.0.0"``

The IP network address mask that decides what interfaces
the web server will bind to.

By default this will bind to all interfaces.

This option is usually set by :option:`faust worker --web-bind`,
not by passing it as a keyword argument to :class:`app`.

.. setting:: web_in_thread

``web_in_thread``
-----------------

.. versionadded:: 1.5

:type: :class:`bool`
:default: :const:`False`

Run the web server in a separate thread.

Use this if you have a large value for :setting:`stream_buffer_maxsize`
and want the web server to be responsive when the worker is otherwise
busy processing streams.

.. note::

    Running the web server in a separate thread means web views
    and agents will not share the same event loop.

.. setting:: web_cors_options

``web_cors_options``
--------------------

.. versionadded:: 1.5

:type: ``Mapping[str, ResourceOptions]``
:default: :const:`None`

Enable `Cross-Origin Resource Sharing`_ options for all web views
in the internal web server.

This should be specified as a dictionary of
URLs to :class:`~faust.web.ResourceOptions`:

.. sourcecode:: python

    app = App(..., web_cors_options={
        'http://foo.example.com': ResourceOptions(
            allow_credentials=True,
            allow_methods='*',
        )
    })

Individual views may override the CORS options used as
arguments to to ``@app.page`` and ``blueprint.route``.

.. seealso::

    :pypi:`aiohttp_cors`: https://github.com/aio-libs/aiohttp-cors

.. _`Cross-Origin Resource Sharing`:
    https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS

.. _settings-agent:

Advanced Agent Settings
=======================

.. setting:: agent_supervisor

``agent_supervisor``
--------------------

:type: :class:`str:`/:class:`mode.SupervisorStrategyT`
:default: :class:`mode.OneForOneSupervisor`

An agent may start multiple instances (actors) when
the concurrency setting is higher than one (e.g.
``@app.agent(concurrency=2)``).

Multiple instances of the same agent are considered to be in the same
supervisor group.

The default supervisor is the :class:`mode.OneForOneSupervisor`:
if an instance in the group crashes, we restart that instance only.

These are the supervisors supported:

+ :class:`mode.OneForOneSupervisor`

    If an instance in the group crashes we restart only that instance.

+ :class:`mode.OneForAllSupervisor`

    If an instance in the group crashes we restart the whole group.

+ :class:`mode.CrashingSupervisor`

    If an instance in the group crashes we stop the whole application,
    and exit so that the Operating System supervisor can restart us.

+ :class:`mode.ForfeitOneForOneSupervisor`

    If an instance in the group crashes we give up on that instance
    and never restart it again (until the program is restarted).

+ :class:`mode.ForfeitOneForAllSupervisor`

    If an instance in the group crashes we stop all instances
    in the group and never restarted them again (until the program is
    restarted).

.. _settings-rpc:

Agent RPC Settings
==================

.. setting:: reply_to

``reply_to``
------------

:type: ``str``
:default: `str(uuid.uuid4())`

The name of the reply topic used by this instance.  If not set one will be
automatically generated when the app is created.

.. setting:: reply_create_topic

``reply_create_topic``
----------------------

:type: ``bool``
:default: :const:`False`

Set this to :const:`True` if you plan on using the RPC with agents.

This will create the internal topic used for RPC replies on that instance
at startup.

.. setting:: reply_expires

``reply_expires``
-----------------

:type: ``Union[float, datetime.timedelta]``
:default: ``timedelta(days=1)``

The expiry time (in seconds :class:`float`, or :class:`~datetime.timedelta`),
for how long replies will stay in the instances local reply topic
before being removed.

.. setting:: reply_to_prefix

``reply_to_prefix``
-------------------

:type: ``str``
:default: ``"f-reply-"``

The prefix used when generating reply topic names.

.. _settings-extending:

Extension Settings
==================

.. setting:: Agent

``Agent``
---------

:type: ``Union[str, Type]``
:default: :class:`faust.Agent`

The :class:`~faust.Agent` class to use for agents, or the fully-qualified
path to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    class MyAgent(faust.Agent):
        ...

    app = App(..., Agent=MyAgent)

Example using the string path to a class::

    app = App(..., Agent='myproj.agents.Agent')

.. setting:: Event

``Event``
---------

:type: ``Union[str, Type]``
:default: :class:`faust.Event`

The :class:`~faust.Event` class to use for creating new event objects,
or the fully-qualified path to one (supported by
:func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    class MyBaseEvent(faust.Event):
        ...

    app = App(..., Event=MyBaseEvent)

Example using the string path to a class::

    app = App(..., Event='myproj.events.Event')

.. setting:: Schema

``Schema``
----------

:type: ``Union[str, Type]``
:default: :class:`faust.Schema`

The :class:`~faust.Schema` class to use as the default
schema type when no schema specified. or the fully-qualified
path to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    class MyBaseSchema(faust.Schema):
        ...

    app = App(..., Schema=MyBaseSchema)

Example using the string path to a class::

    app = App(..., Schema='myproj.schemas.Schema')

.. setting:: Stream

``Stream``
----------

:type: ``Union[str, Type]``
:default: :class:`faust.Stream`

The :class:`~faust.Stream` class to use for streams, or the fully-qualified
path to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    class MyBaseStream(faust.Stream):
        ...

    app = App(..., Stream=MyBaseStream)

Example using the string path to a class::

    app = App(..., Stream='myproj.streams.Stream')

.. setting:: Table

``Table``
---------

:type: ``Union[str, Type[TableT]]``
:default: :class:`faust.Table`

The :class:`~faust.Table` class to use for tables, or the fully-qualified
path to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    class MyBaseTable(faust.Table):
        ...

    app = App(..., Table=MyBaseTable)

Example using the string path to a class::

    app = App(..., Table='myproj.tables.Table')

.. setting:: SetTable

``SetTable``
------------

:type: ``Union[str, Type[TableT]]``
:default: :class:`faust.SetTable`

The :class:`~faust.SetTable` class to use for table-of-set tables,
or the fully-qualified path to one (supported
by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    class MySetTable(faust.SetTable):
        ...

    app = App(..., Table=MySetTable)

Example using the string path to a class::

    app = App(..., Table='myproj.tables.MySetTable')

.. setting:: GlobalTable

``GlobalTable``
---------------

:type: ``Union[str, Type[GlobalTableT]]``
:default: :class:`faust.GlobalTable`

The :class:`~faust.GlobalTable` class to use for tables, or the fully-qualified
path to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    class MyBaseGlobalTable(faust.GlobalTable):
        ...

    app = App(..., GlobalTable=MyBaseGlobalTable)

Example using the string path to a class::

    app = App(..., GlobalTable='myproj.tables.GlobalTable')

.. setting:: SetGlobalTable

``SetGlobalTable``
------------------

:type: ``Union[str, Type[GlobalTableT]]``
:default: :class:`faust.SetGlobalTable`

The :class:`~faust.SetGlobalTable` class to use for tables, or the
fully-qualified path to one (supported by
:func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    class MyBaseSetGlobalTable(faust.SetGlobalTable):
        ...

    app = App(..., SetGlobalTable=MyBaseGlobalSetTable)

Example using the string path to a class::

    app = App(..., SetGlobalTable='myproj.tables.SetGlobalTable')

.. setting:: TableManager

``TableManager``
----------------

:type: ``Union[str, Type[TableManagerT]]``
:default: :class:`faust.tables.TableManager`

The :class:`~faust.tables.TableManager` used for managing tables,
or the fully-qualified path to one (supported by
:func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    from faust.tables import TableManager

    class MyTableManager(TableManager):
        ...

    app = App(..., TableManager=MyTableManager)

Example using the string path to a class::

    app = App(..., TableManager='myproj.tables.TableManager')

.. setting:: Serializers

``Serializers``
---------------

:type: ``Union[str, Type[RegistryT]]``
:default: :class:`faust.serializers.Registry`

The :class:`~faust.serializers.Registry` class used for
serializing/deserializing messages; or the fully-qualified path
to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    from faust.serialiers import Registry

    class MyRegistry(Registry):
        ...

    app = App(..., Serializers=MyRegistry)

Example using the string path to a class::

    app = App(..., Serializers='myproj.serializers.Registry')

.. setting:: Worker

``Worker``
----------

:type: ``Union[str, Type[WorkerT]]``
:default: :class:`faust.Worker`

The :class:`~faust.Worker` class used for starting a worker
for this app; or the fully-qualified path
to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    import faust

    class MyWorker(faust.Worker):
        ...

    app = faust.App(..., Worker=Worker)

Example using the string path to a class::

    app = faust.App(..., Worker='myproj.workers.Worker')

.. setting:: PartitionAssignor

``PartitionAssignor``
---------------------

:type: ``Union[str, Type[PartitionAssignorT]]``
:default: :class:`faust.assignor.PartitionAssignor`

The :class:`~faust.assignor.PartitionAssignor` class used for assigning
topic partitions to worker instances; or the fully-qualified path
to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    from faust.assignor import PartitionAssignor

    class MyPartitionAssignor(PartitionAssignor):
        ...

    app = App(..., PartitionAssignor=PartitionAssignor)

Example using the string path to a class::

    app = App(..., Worker='myproj.assignor.PartitionAssignor')

.. setting:: LeaderAssignor

``LeaderAssignor``
------------------

:type: ``Union[str, Type[LeaderAssignorT]]``
:default: :class:`faust.assignor.LeaderAssignor`

The :class:`~faust.assignor.LeaderAssignor` class used for assigning
a master Faust instance for the app; or the fully-qualified path
to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    from faust.assignor import LeaderAssignor

    class MyLeaderAssignor(LeaderAssignor):
        ...

    app = App(..., LeaderAssignor=LeaderAssignor)

Example using the string path to a class::

    app = App(..., Worker='myproj.assignor.LeaderAssignor')

.. setting:: Router

``Router``
----------

:type: ``Union[str, Type[RouterT]]``
:default: :class:`faust.app.router.Router`

The :class:`~faust.router.Router` class used for routing requests
to a worker instance having the partition for a specific key (e.g. table key);
or the fully-qualified path to one (supported by
:func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    from faust.router import Router

    class MyRouter(Router):
        ...

    app = App(..., Router=Router)

Example using the string path to a class::

    app = App(..., Router='myproj.routers.Router')

.. setting:: Topic

``Topic``
---------

:type: ``Union[str, Type[TopicT]]``
:default: :class:`faust.Topic`

The :class:`~faust.Topic` class used for defining new topics; or the
fully-qualified path to one (supported by
:func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    import faust

    class MyTopic(faust.Topic):
        ...

    app = faust.App(..., Topic=MyTopic)

Example using the string path to a class::

    app = faust.App(..., Topic='myproj.topics.Topic')

.. setting:: HttpClient

``HttpClient``
--------------

:type: ``Union[str, Type[HttpClientT]]``
:default: :class:`aiohttp.client.ClientSession`

The :class:`aiohttp.client.ClientSession` class used as a HTTP client; or the
fully-qualified path to one (supported by
:func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    import faust
    from aiohttp.client import ClientSession

    class HttpClient(ClientSession):
        ...

    app = faust.App(..., HttpClient=HttpClient)

Example using the string path to a class::

    app = faust.App(..., HttpClient='myproj.http.HttpClient')

.. setting:: Monitor

``Monitor``
-----------

:type: ``Union[str, Type[SensorT]]``
:default: :class:`faust.sensors.Monitor`

The :class:`~faust.sensors.Monitor` class as the main sensor
gathering statistics for the application; or the
fully-qualified path to one (supported by
:func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    import faust
    from faust.sensors import Monitor

    class MyMonitor(Monitor):
        ...

    app = faust.App(..., Monitor=MyMonitor)

Example using the string path to a class::

    app = faust.App(..., Monitor='myproj.monitors.Monitor')

