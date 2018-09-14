.. _changelog:

==============================
 Change history for Faust 1.1
==============================

This document contain change notes for bugfix releases in
the Faust 1.1 series. If you're looking for previous releases,
please visit the :ref:`history` section.

.. _version-1.1.0:

1.1.0
=====
:release-date: 2018-09-14 1:07 P.M PDT
:release-by: Ask Solem (:github_user:`ask`)

.. _v110-important-notes:

Important Notes
---------------

- **API**: Agent/Channel.send now support keyword-only arguments only

    Users often make the mistake of doing:

    .. sourcecode:: python

        channel.send(x)

    and expect that to send ``x`` as the value.

    But the signature is ``(key, value, ...)``, so it ends up being
    ``channel.send(key=x, value=None)``.

    Fixing this will come in two parts:

    1) Faust 1.1 (this change): Make them keyword-only arguments

        This will make it an error if the names of arguments are not
        specified:

        .. sourcecode:: python

            channel.send(key, value)

        Needs to be changed to:

        .. sourcecode:: python

            channel.send(key=key, value=value)

    2) Faust 1.2: We will change the signature
        to ``channel.send(value, key=key, ...)``

        At this stage all existing code will have changed to using
        keyword-only arguments.

- **App**: The default key serializer is now ``raw`` (Issue #142).

    The default *value* serializer will still be ``json``, but for keys
    it does not make as much sense to use json as the default: keys are very
    rarely expressed using complex structures.

    If you depend on the Faust 1.0 behavior you should override the
    default key serializer for the app:

    .. sourcecode:: python

        app = faust.App('myapp', ..., key_serializer='json')

    Contributed by Allison Wang (:github_user:`allisonwang`)

- No longer depends on :pypi:`click_completion`

        If you want to use the shell completion command,
        you now have to install that dependency locally first:

        .. sourcecode:: console

            $ ./examples/withdrawals.py completion
            Usage: withdrawals.py completion [OPTIONS]

            Error: Missing required dependency, but this is easy to fix.
            Run `pip install click_completion` from your virtualenv
            and try again!

        Installing :pypi:`click_completion`:

        .. sourcecode:: console

            $ pip install click_completion
            [...]

.. _v110-news:

News
----

- **Requirements**

    + Now depends on :ref:`Mode 1.17.1 <mode:version-1.17.1>`.

    + No longer depends on :pypi:`click_completion`

- Now works with CPython 3.6.0.

- **Models**: Record: Now supports `decimals` option to convert string
  decimals back to Decimal

    This can be used for any model to enable "Decimal-fields":

    .. code-block:: python

        class Fundamentals(faust.Record, decimals=True):
            open: Decimal
            high: Decimal
            low: Decimal
            volume: Decimal

    When serialized this model will use string for decimal fields
    (the javascript float type cannot be used without losing precision, it
    is a float after all), but when deserializing Faust will reconstruct
    them as Decimal objects from that string.

- **Model**: Records now support custom coercion handlers.

    Coercion converts one type into another, for example from string to
    :class:`~datetime.datettime`, or int/string to :class:`~decimal.Decimal`.

    In models this means conversion from the serialized form back into
    a corresponding Python type.

    To define a model where all :class:`~uuid.UUID` fields are serialized
    to string, but then converted back to :class:`~uuid.UUID` objects
    when deserialized, do this:

    .. sourcecode:: python

        from uuid import UUID
        import faust

        class Account(faust.Record, coercions={UUID: UUID}):
            id: UUID

    .. admonition:: What about non-json serializable types?

        The use of UUID in this example leaves one important detail
        out: json doesn't support this type so how can models serialize it?

        The Faust JSON serializer adds support for UUID objects by default,
        but if you have a custom class you would need to add that capability
        by adding a ``__json__`` handler:

        .. sourcecode:: python

            class MyType:

                def __init__(self, value: str):
                    self.value = value

                def __json__(self):
                    return self.value

    You'd get tired writing this out for every class, so why not make
    an abstract model subclass:

    .. sourcecode:: python

        from uuid import UUID
        import faust

        class UUIDAwareRecord(faust.Record,
                              abstract=True,
                              coercions={UUID: UUID}):
            ...

        class Account(UUIDAwareRecord):
            id: UUID

- **App**: New :setting:`ssl_context` adds authentication support to Kafka.

    Contributed by Mika Eloranta (:github_user:`melor`).

- **Monitor**: New `Datadog`_ monitor (Issue #160)

    Contributed by Allison Wang (:github_user:`allisonwang`).

    .. _`Datadog`: http://datadoghq.com

- **App**: ``@app.task`` decorator now accepts ``on_leader``
           argument (Issue #131).

    Tasks created using the ``@app.task`` decorator will run once a worker
    is fully started.

    Similar to the ``@app.timer`` decorator, you can now create one-shot tasks
    that run on the leader worker only:

    .. sourcecode:: python

        @app.task(on_leader=True)
        async def mytask():
            print('WORKER STARTED, AND I AM THE LEADER')

    The decorated function may also accept the ``app`` as an argument:

    .. sourcecode:: python

        @app.task(on_leader=True)
        async def mytask(app):
            print(f'WORKER FOR APP {app} STARTED, AND I AM THE LEADER')

- **App**: New ``app.producer_only`` attribute.

    If set the worker will start the app without
    consumer/tables/agents/topics.

- **App**: ``app.http_client`` property is now read-write.

- **Channel**: In-memory channels were not working as expected.

    + ``Channel.send(key=key, value=value)`` now works as expected.

    + ``app.channel()`` accidentally set the ``maxsize`` to 1 by default,
      creating a deadlock.

    + ``Channel.send()`` now disregards the :setting:`stream_publish_on_commit`
      setting.

- **Transport**: :pypi:`aiokafka`: Support timestamp-less messages

    Fixes error when data sent with old Kafka broker not supporting
    timestamps:

    .. code-block:: text

        [2018-08-27 08:00:49,262: ERROR]: [^--Consumer]: Drain messages raised:
            TypeError("unsupported operand type(s) for /: 'NoneType' and 'float'",)
        Traceback (most recent call last):
        File "faust/transport/consumer.py", line 497, in _drain_messages
            async for tp, message in ait:
        File "faust/transport/drivers/aiokafka.py", line 449, in getmany
            record.timestamp / 1000.0,
        TypeError: unsupported operand type(s) for /: 'NoneType' and 'float'

    Contributed by Mika Eloranta (:github_user:`melor`).

- **Distribution**: ``pip install faust`` no longer installs the examples
  direcrtory.

    Fix contributed by Michael Seifert (:github_user:`seifertm`)

- **Web**: Adds exception handling to views.

    A view can now bail out early via `raise self.NotFound()` for example.

- **Web**: ``@table_route`` decorator now supports taking key from
  the URL path.

    This is now used in the :file:`examples/word_count.py` example
    to add an endpoint ``/count/{word}/`` that routes to the correct
    worker with that count:

    .. sourcecode:: python

        @app.page('/word/{word}/count/')
        @table_route(table=word_counts, match_info='word')
        async def get_count(web, request, word):
            return web.json({
                word: word_counts[word]
            })

- **Web**: Support reverse lookup from view name via ``url_for``

    .. sourcecode:: python

        web.url_for(view_name, **params)

- **Web**: Adds support for Flask-like "blueprints"

    Blueprint is basically just a description of a reusable app
    that you can add to your web application.

    Blueprints are commonly used in most Flask-like web frameworks,
    but Flask blueprints are not compatible with e.g. Sanic blueprints.

    The Faust blueprint is not directly compatible with any of them,
    but that should be fine.

    To define a blueprint:

    .. sourcecode:: python

        from faust import web

        blueprint = web.Blueprint('user')

        @blueprint.route('/', name='list')
        class UserListView(web.View):

            async def get(self, request: web.Request) -> web.Response:
                return self.json({'hello': 'world'})

        @blueprint.route('/{username}/', name='detail')
        class UserDetailView(web.View):

            async def get(self, request: web.Request) -> web.Response:
                name = request.match_info['username']
                return self.json({'hello': name})

            async def post(self, request: web.Request) -> web.Response:
                ...

            async def delete(self, request: web.Request) -> web.Response:
                ...

    Then to add the blueprint to a Faust app you register it:

    .. sourcecode:: python

        blueprint.register(app, url_prefix='/users/')

    .. note::

        You can also create views from functions (in this case it will only
        support GET):

        .. sourcecode:: python

            @blueprint.route('/', name='index')
            async def hello(self, request):
                return self.text('Hello world')

    .. admonition:: Why?

        Asyncio web frameworks are moving quickly, and we want to be able
        to quickly experiment with different backend drivers.

        Blueprints is a tiny abstraction that fit well into the already
        small web abstraction that we do have.

    - Documentation and examples improvements by

        + Tom Forbes (:github_user:`orf`).
        + Matthew Grossman (:github_user:`matthewgrossman`)
        + Denis Kataev (:github_user:`kataev`)
        + Allison Wang (:github_user:`allisonwang`)
        + Huyuumi (:github_user:`diplozoon`)

Project
-------

- **CI**: The following Python versions have been added to the build matrix:

    + CPython 3.7.0

    + CPython 3.6.6

    + CPython 3.6.0

- **Git**:

    + All the version tags have been cleaned up to follow the format ``v1.2.3``.

    + New active maintenance branches: ``1.0`` and ``1.1``.
