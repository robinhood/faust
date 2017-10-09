.. _guide-models:

=====================================
 Models and Serialization
=====================================

.. contents::
    :local:
    :depth: 1

.. module:: faust

.. currentmodule:: faust

Basics
======

Models describe how to serialize/deserialize message keys and values,
using modern Python type annotation syntax.

It's important to note that you don't actually need to use models to
describe the content of your messages, streams can happily take any
value or even work with raw byte strings, but models add value
by serving as documentation and enabling static type checks of
for example JSON dictionary fields.

If you're integrating with existing systems Faust's :ref:`codecs
<codecs-guide>` can help you support serialization and deserialization
in any format.  Models describe the content of messages, while codecs describe
how they are serialized/compressed/encoded/etc.
The default codec is configured by the applications ``key_serializer`` and
``value_serializer`` arguments, and individual model may override the default
by specifying the ``serializer`` argument when creating the model class,
for example:

.. sourcecode:: python

    class MyRecord(Record, serializer='json')
        ...

Codecs can also be combined so that they consist of multiple encoding/decoding
stages, for example data serialized with JSON and then Base64 encoded would
be ``serializer='json|binary'``.

.. seealso::

    See the :ref:`guide-codecs` section for information about codecs
    and how to define your own.

.. topic:: Sending/receiving raw values

    Sending/receiving non-described keys and values is easy:

    .. sourcecode:: python

        # examples/nondescript.py
        import faust

        app = faust.App('values')
        transfers_topic = app.topic('transfers')
        large_transfers_topic = app.topic('large_transfers')

        @app.agent(transfers_topic)
        async def find_large_transfers(transfers):
            async for transfer in transfers:
                if transfer['amount'] > 1000.0:
                    await large_transfers_topic.send(value=transfer)

        async def send_transfer(account_id, amount):
            await transfers_topic.send(value={
                'account_id': account_id,
                'amount': amount,
            })

Using models to describe topics gives benefits:

.. sourcecode:: python

    # examples/described.py
    import faust

    class Transfer(faust.Record):
        account_id: str
        amount: float

    app = faust.app('values')
    transfers_topic = app.topic('transfers', value_type=Transfer)
    large_transfers_topic = app.topic('large_transfers', value_type=Transfer)

    @app.agent(transfers_topic)
    async def find_large_transfers(transfers):
        async for transfer in transfers:
            if transfer.amount > 1000.0:
                await large_transfers_topic.send(value=transfer)

    async def send_transfer(account_id, amount):
        await transfers_topic.send(
            value=Transfer(account_id=account_id, amount=amount),
        )

Using static type checks you can now be alerted if something sends
the wrong type of value for the `account_id` for example.
The most compelling reason for using non-described messages would
be to integrate with existing Kafka topics and systems, but if you're
writing new systems in Faust the best practice would be to describe models.

Model Types
===========

The first version of Faust only supports dictionary models (records),
but can easily extended to support other types of models, like arrays.

Records
-------

A record is a model based on a dictionary/mapping.

Here's a simple record describing a 2d point, with two required fields: ``x``
and ``y``:

.. sourcecode:: python

    class Point(faust.Record):
        x: int
        y: int

To create a new point, instantiate it like a regular Python object.  The
fields are provided as keyword arguments:

.. sourcecode:: pycon

    >>> point = Point(x=10, y=20)
    >>> point
    <Point: x=10, y=20>

Attempting to instantiate a model without providing a value for a required
field is an error:

.. sourcecode:: pycon

    >>> point = Point(x=10)

.. sourcecode:: pytb

    Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
    File "/opt/devel/faust/faust/models/record.py", line 96, in __init__
        self._init_fields(fields)
    File "/opt/devel/faust/faust/models/record.py", line 106, in _init_fields
        type(self).__name__, ', '.join(sorted(missing))))
    TypeError: Point missing required arguments: y

.. note::

    The annotations are used for static analysis only, there's no type checking
    performed at runtime.

To describe an optional field, simply set a default value:

.. sourcecode:: python

    class Point(faust.Record, serializer='json'):
        x: int
        y: int = 0

You can now omit the ``y`` field when creating a new point:

.. sourcecode:: pycon

    >>> point = Point(x=10)
    >>> point
    <Point: x=10, y=0>

We can now use ``Point`` objects as message keys, and message values:

.. sourcecode:: python

    await app.send('mytopic', key=Point(x=10, y=20), value=Point(x=30, y=10))

The above will send a message to Kafka, to have a stream automatically
deserialize these points, use ``faust.topic`` to describe a topic as having
points as key and value types:

.. sourcecode:: python

    my_topic = faust.topic('mytopic', key_type=Point, value_type=Point)

    @app.agent(my_topic)
    async def task(events):
        async for event in events:
            print(event)

Records can also have other records as fields:

.. sourcecode:: python

    class Account(faust.Record, serializer='json'):
        id: str
        balance: float

    class Transfer(faust.Record, serializer='json'):
        account: Account
        amount: float

    transfer = Transfer(
        account=Account(id='RBH1235678', balance=13000.0),
        amount=1000.0,
    )

To manually serialize a record use its ``.dumps()`` method:

.. sourcecode:: python

    >>> json = transfer.dumps()

To convert the JSON back into a model use the ``.loads()`` class method:

.. sourcecode:: pycon

    >>> transfer = Transfer.loads(json)

Reference
~~~~~~~~~

Serialization/Deserialization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. class:: Record
    :noindex:

    .. automethod:: loads
        :noindex:

    .. automethod:: dumps
        :noindex:

    .. automethod:: to_representation
        :noindex:

Schemas
^^^^^^^

.. class:: Record
    :noindex:

    .. automethod:: as_schema
        :noindex:

    .. automethod:: as_avro_schema
        :noindex:

.. _guide-codecs:

Codecs
======

Supported codecs
----------------

* **raw**     - no encoding/serialization (bytes only).
* **json**    - json with utf-8 encoding.
* **pickle**  - pickle with base64 encoding (not urlsafe).
* **binary**  - base64 encoding (not urlsafe).

Serialization by name
---------------------

The func:`dumps` function takes a codec name and the object to encode,
the return value is bytes:

.. sourcecode:: pycon

    >>> s = dumps('json', obj)

For the reverse direction, the func:`loads` function takes a codec
name and an encoded payload to decode (bytes):

.. sourcecode:: pycon

    >>> obj = loads('json', s)

You can also combine encoders in the name, like in this case
where json is combined with gzip compression:

.. sourcecode:: pycon

    >>> obj = loads('json|gzip', s)

Codec registry
--------------

Codecs are configured by name and the :mod:`faust.serializers.codecs` module
maintains a mapping from name to :class:`Codec` instance: the :attr:`codecs`
attribute.

You can add a new codec to this mapping by:

.. sourcecode:: pycon

    >>> from faust.serializers import codecs
    >>> codecs.register(custom, custom_serializer())

A codec subclass requires two methods to be implemented: ``_loads()``
and ``_dumps()``:

.. sourcecode:: python

    import msgpack

    from faust.serializers import codecs

    class raw_msgpack(codecs.Codec):

        def _dumps(self, obj: Any) -> bytes:
            return msgpack.dumps(obj)

        def _loads(self, s: bytes) -> Any:
            return msgpack.loads(s)

Our codec now encodes/decodes to raw msgpack format, but we
may also need to transfer this payload on a transport not
handling binary data well.  Codecs may be chained together,
so to add a text encoding like base64, which we use in this case,
we use the ``|`` operator to form a combined codec:

.. sourcecode:: python

    def msgpack() -> codecs.Codec:
        return raw_msgpack() | codecs.binary()

    codecs.register('msgpack', msgpack())

At this point we monkey-patched Faust to support
our codec, and we can use it to define records:

.. sourcecode:: pycon

    >>> from faust import Record
    >>> class Point(Record, serializer='msgpack'):
    ...     x: int
    ...     y: int

The problem with monkey-patching is that we must make sure the patching
happens before we use the feature.

Faust also supports registering *codec extensions*
using setuptools entrypoints, so instead we can create an installable msgpack
extension.

To do so we need to define a package with the following directory layout:

.. sourcecode:: text

    faust-msgpack/
        setup.py
        faust_msgpack.py

The first file, :file:`faust-msgpack/setup.py`, defines metadata about our
package and should look like the following example:

.. sourcecode:: python

    import setuptools

    setuptools.setup(
        name='faust-msgpack',
        version='1.0.0',
        description='Faust msgpack serialization support',
        author='Ola A. Normann',
        author_email='ola@normann.no',
        url='http://github.com/example/faust-msgpack',
        platforms=['any'],
        license='BSD',
        packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
        zip_safe=False,
        install_requires=['msgpack-python'],
        tests_require=[],
        entry_points={
            'faust.codecs': [
                'msgpack = faust_msgpack:msgpack',
            ],
        },
    )

The most important part being the ``entry_points`` key which tells
Faust how to load our plugin. We have set the name of our
codec to ``msgpack`` and the path to the codec class
to be ``faust_msgpack:msgpack``. This will be imported by Faust
as ``from faust_msgpack import msgpack``, so we need to define
that part next in our :file:`faust-msgpack/faust_msgpack.py` module:

.. sourcecode:: python

    from faust.serializers import codecs

    class raw_msgpack(codecs.Codec):

        def _dumps(self, obj: Any) -> bytes:
            return msgpack.dumps(s)


    def msgpack() -> codecs.Codec:
        return raw_msgpack() | codecs.binary()

That's it! To install and use our new extension we do:

.. sourcecode:: console

    $ python setup.py install

At this point may want to publish this on PyPI to share
the extension with other Faust users.
