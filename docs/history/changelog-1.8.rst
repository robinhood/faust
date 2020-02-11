.. _changelog-1.8:

==============================
 Change history for Faust 1.8
==============================

This document contain change notes for bugfix releases in
the Faust 1.8.x series. If you're looking for changes in the latest
series, please visit the latest :ref:`changelog`.

For even older releases you can visit the :ref:`history` section.

.. _version-1.8.1:

1.8.1
=====
:release-date: 2019-10-17 1:10 P.M PST
:release-by: Ask Solem (:github_user:`ask`)

- **Requirements**

    + Now depends on :ref:`Mode 4.1.2 <mode:version-4.1.2>`.

- **Tables**: Fixed bug in table route decorator introduced in 1.8
  (Issue #434).

    Fix contributed by Vikram Patki (:github_user:`patkivikram`).

- **Stream**: Now properly acks :const:`None` values (tombstone messages).

    Fix contributed by Vikram Patki (:github_user:`patkivikram`).

- **Tables**: Fixed bug with ``use_partitioner`` when destination
  partitin is ``0`` (Issue #447).

    Fix contributed by Tobias Rauter (:github_user:`trauter`).

- **Consumer**: Livelock warning is now per TopicPartition.

- **Cython**: Add missing attribute in Cython class

  Fix contributed by Martin Maillard (:github_user:`martinmaillard`).

- **Distribution**: Removed broken gevent support
  (Issue #182, Issue #272).

  Removed all traces of gevent as the ``aiogevent`` project has been removed
  from PyPI.

  Contributed by Bryant Biggs (:github_user:`bryantbiggs`).

- Documentation fixes by:

  + Bryant Biggs (:github_user:`bryantbiggs`).

.. _version-1.8.0:

1.8.0
=====
:release-date: 2019-09-27 4:05 P.M PST
:release-by: Ask Solem (:github_user:`ask`)


- **Requirements**

    + Now depends on :ref:`Mode 4.1.0 <mode:version-4.1.0>`.

- **Tables**: New "global table" support (Issue #366).

  A global table is a table where all worker instances
  maintain a copy of the full table.

  This is useful for smaller tables that need to be
  shared across all instances.

  To define a new global table use ``app.GlobalTable``:

  .. sourcecode:: python

    global_table = app.GlobalTable('global_table_name')

  Contributed by Artak Papikyan (:github_user:`apapikyan`).

- **Transports**: Fixed hanging when Kafka topics have gaps
  in source topic offset (Issue #401).

    This can happen when topics are compacted or similar,
    and Faust would previously hang when encountering
    offset gaps.

    Contributed by Andrei Tuppitcyn (:github_user:`andr83`).

- **Tables**: Fixed bug with crashing when key index enabled (Issue #414).

- **Streams**: Now properly handles exceptions in ``group_by``.

    Contributed by Vikram Patki (:github_user:`patkivikram`).

- **Streams**: Fixed bug with ``filter`` not acking messages (Issue #391).

    Fix contributed by Martin Maillard (:github_user:`martinmaillard`).

- **Web**: Fixed typo in ``NotFound`` error.

    Fix contributed by Sanyam Satia (:github_user:`ssatia`).

- **Tables**: Added ``use_partitioner`` option for the ability
  to modify tables outside of streams (for example HTTP views).

    By default tables will use the partition number of a "source event"
    to write an entry to the changelog topic.

    This means you can safely modify tables in streams:

    .. sourcecode:: python

        @app.agent()
        async def agent(stream):
            async for key, value in stream.items():
                table[key] = value

   when the table is modified it will know what topic the source
   event comes from and use the same partition number.

   An alternative to this form of partitioning is to use
   the Kafka default partitioner on the key, and now you can
   use that strategy by enabling the ``use_partitioner`` option:

   .. sourcecode:: python

        my_table = app.Table('name', use_partitioner=True)

    You may also temporarily enable this option in any location
    by using ``table.clone(use_paritioner=True)``:

    .. sourcecode:: python

        @app.page('/foo/{key}/')
        async def foo(web, request, key):
            table.clone(use_partitoner)[key] = 'bar'

- **Models**: Support for "schemas" that group key/value related
  settings together (Issue #315).

   This implements a single structure (Schema) that configures
   the ``key_type``/``value_type``/``key_serializer``/``value_serializer``
   for a topic or agent:

   .. sourcecode:: python

        class Point(faust.Record):
            x: int
            y: int
            z: int = None

        schema = faust.Schema(
            key_type=Point,
            value_type=Point,
            key_serializer='json',
            value_serializer='json',
        )

        topic = app.topic('mytopic', schema=schema)

    The benefit of having an abstraction a level above codecs
    is that schemas can implement support for serialization formats
    such as ProtocolBuffers, Apache Thrift and Avro.

    The schema will also have access to the Kafka message headers,
    necessary in some cases where serialization schema is specified
    in headers.

    .. seealso::

        :ref:`model-schemas` for more information.

- **Models**: Validation now supports optional fields (Issue #430).

- **Models**: Fixed support for ``Optional`` and field coercion
  (Issue #393).

    Fix contributed by Martin Maillard (:github_user:`martinmaillard`).

- **Models**: Manually calling ``model.validate()`` now also
  validates that the value is of the correct type (Issue #425).

- **Models**: Fields can now specify ``input_name`` and ``output_name``
  to support fields named after Python reserved keywords.

    For example if the data you want to parse contains a field
    named ``in``, this will not work since :keyword:`in` is
    a reserved keyword.

    Using the new ``input_name`` feature you can rename the field
    to something else in Python, while still serializing/deserializing
    to the existing field:

    .. sourcecode:: python

        from faust.models import Record
        from faust.models.fields import StringField

        class OpenAPIParameter(Record):
            location: str = StringField(default='query', input_name='in')

    ``input_name`` is the name of the field in serialized data,
    while ``output_name`` is what the field will be named when you
    serialize this model object:

    .. sourcecode:: pycon

        >>> import json

        >>> data = {'in': 'header'}
        >>> parameter = OpenAPIParameter.loads(json.dumps(data))
        >>> assert parameter.location == 'header'
        >>> parameter.dumps(serialier='json')
        '{"in": "header"}'

    .. note::

        - The default value for ``input_name`` is the name of the field.
        - The default value for ``output_name`` is the value of
          ``input_name``.

- **Models**: now have a ``lazy_creation`` class option to delay
  class initialization to a later time.

    Field types are described using Python type annotations,
    and model fields can refer to other models, but not always
    are those models defined at the time when the class is defined.

    Such as in this example:

    .. sourcecode:: python

        class Foo(Record):
           bar: 'Bar'

        class Bar(Record):
           foo: Foo

    This example will result in an error, since trying to resolve
    the name ``Bar`` when the class ``Foo`` is created is impossible
    as that class does not exist yet.

    In this case we can enable the ``lazy_creation`` option:

    .. sourcecode:: python

        class Foo(Record, lazy_creation=True):
            bar: 'Bar'

        class Bar(Record):
            foo: Foo

        Foo.make_final()  # <-- 'Bar' is now defined so safe to create.

- **Transports**: Fixed type mismatch in :pypi:`aiokafka` ``timestamp_ms``

    Contributed by :github_user:`ekerstens`.

- **Models**: Added YAML serialization support.

    This requires the :pypi:`PyYAML` library.

- **Sensors**: Added HTTP monitoring of status codes and latency.

- **App**: Added new :setting:`Schema` setting.

- **App**: Added new :setting:`Event` setting.

- **Channel**: A new :class:`~faust.channels.SerializedChannel`
  subclass can now be used to define new channel types that need
  to deserialize incoming messages.

- **Cython**: Added missing field declaration.

  Contributed by Victor Miroshnikov (:github_user:`superduper`)

- Documentation fixes by:

  + Adam Bannister (:github_user:`AtomsForPeace`).

  + Roman Imankulov (:github_user:`imankulov`).

  + Espen Albert (:github_user:`EspenAlbert`).

  + Alex Zeecka (:github_user:`Zeecka`).

  + Victor Noagbodji (:github_user:`nvictor`).

  + (:github_user:`imankulov`).

  + (:github_user:`Zeecka`).

