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

Models describe the fields of data structures used as keys and values
in messages.  They're defined using a ``NamedTuple``-like syntax,
as introduced by Python 3.6, and look like this::

    class Point(Record, serializer='json'):
        x: int
        y: int

This is a point record with ``x``, and ``y`` fields of type int. There's
no type checking at runtime, but the :pypi:`mypy` type checker can be used
as a separate build step, to verify that the right types are provided.

A record is a model of the dictionary type, and describes keys and values.
When using JSON as the serialization format, the Point model above
serializes as:

.. sourcecode:: python

    >>> Point(x=10, y=100).dumps()
    {"x": 10, "y": 100}

You can also specify a different serializer as an argument to ``.dumps``:

.. sourcecode:: python

    >>> Point(x=10, y=100).dumps('pickle')  # pickle + Base64
    b'gAN9cQAoWAEAAAB4cQFLClgBAAAAeXECS2RYBwAAAF9fZmF1c3RxA31xBFg
    CAAAAbnNxBVgOAAAAX19tYWluX18uUG9pbnRxBnN1Lg=='

"Record" is the only model type supported by this version of Faust,
but is just one of many possible model types to include in the future.
The nomenclature is based on the Avro serialization description format,
that supports records, arrays, and more.

Manual Serialization
====================

You're not required to define models to read the data from a stream.
Manual de-serialization also works and is rather easy to perform, but models
provide additional benefit, such as the field descriptors that let you refer
to fields in `group_by` statements, static typing using :pypi:`mypi`,
automatic conversion of :class:`datetime`, and so on...

To deserialize streams manually, simply use a topic with bytes values:

.. sourcecode:: python

    topic = app.topic('custom', value_type=bytes)

    @app.agent
    async def processor(stream):
        async for payload in stream:
            data = json.loads(payload)

If you're integrating with an existing systems Faust's :ref:`codecs`
can help you support serialization and de-serialization
to and from any format.  Models describe the format of messages, while codecs describe
how they're serialized/compressed/encoded/etc.
The default codec is configured by the applications ``key_serializer`` and
``value_serializer`` arguments::

    app = faust.App(key_serializer='json')

and any individual model may override the default
by specifying the ``serializer`` argument when creating the model class.
For example:

.. sourcecode:: python

    class MyRecord(Record, serializer='json')
        ...

Codecs can also be combined so they consist of multiple encoding and decoding
stages, for example data serialized with JSON and then Base64 encoded would
be described as the keyword argument ``serializer='json|binary'``.

.. seealso::

    See the :ref:`codecs` section for information about codecs
    and how to define your own.

.. topic:: Sending/receiving raw values

    Serializing/deserializing keys and values manually is easy.
    The JSON codec happily accepts lists and dictionaries,
    as arguments to the ``.send`` methods:

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

The :pypi:`mypy` static type analyzer can now be alert you if your
code is passing the wrong type of value for the ``account_id`` field,
and more.  The most compelling reason for using non-described messages would
be to integrate with existing Kafka topics and systems, but if you're
writing new systems in Faust the best practice would be to describe models
for your message data.

Model Types
===========

The first version of Faust only supports dictionary models (records),
but can be easily extended to support other types of models, like arrays.

Records
-------

A record is a model based on a dictionary/mapping.  The storage
used is a dictionary, and it serializes to a dictionary, but the same
is true for normal Python objects and their ``__dict__`` storage, so you can
consider record models to be "objects" that can have methods and properties.

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

The above will send a message to Kafka, and whatever receives that
message must be able to deserialize the data.

To define an agent that is able to do so, define the topic to have
specific key/value types:

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

    >>> transfer = Transfer.loads(json_bytes_data)


Lists of lists, etc.
~~~~~~~~~~~~~~~~~~~~

Records can also have fields that are a list of other models, or mappings
to other models, and these are also described using the type annotation
syntax.

To define a model that points to a list of Account objects you can do this:

.. sourcecode:: python

    from typing import List
    import faust


    class LOL(faust.Record):
        accounts: List[Account]

This works with most of the iterable types, so for a list all
of :class:`~typing.Sequence`, :class:`~typing.MutableSequence`, and :class:`~typing.List`
can be used. For a full list of generic data types
recognized by Faust consult the following table:

=========== =================================================================
Collection  Recognized Annotations
=========== =================================================================
List        - :class:`List[ModelT] <typing.List>`
            - :class:`Sequence[ModelT] <typing.Sequence>`
            - :class:`MutableSequence[ModelT] <typing.MutableSequence>`

Set         - :class:`AbstractSet[ModelT] <typing.AbstractSet>`
            - :class:`Set[ModelT] <typing.Set>`
            - :class:`MutableSet[ModelT] <typing.MutableSet>`

Tuple       - :class:`Tuple[ModelT, ...] <typing.Tuple>`
            - :class:`Tuple[ModelT, ModelT, str] <typing.Tuple>`

Mapping     - :class:`Dict[KT, ModelT] <typing.Dict>`
            - :class:`Dict[ModelT, ModelT] <typing.Dict>`
            - :class:`Mapping[KT, ModelT] <typing.Mapping>`
            - :class:`MutableMapping[KT, ModelT] <typing.MutableMapping>`
=========== =================================================================


So from this table we can see that we could have a mapping
of username to account as well:

.. sourcecode:: python

    from typing import Mapping
    import faust

    class DOA(faust.Record):
        accounts: Mapping[str, Account]

and Faust will automatically reconstruct the ``DOA.accounts`` field into
a mapping of string to ``Account`` objects.


There are limitations of this, Faust may not recognize your custom
mapping or list type, so use only what is found in this table.


Automatic conversion of datetimes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Faust will automatically serialize datetimes fields to ISO-8601 string
format, but will not automatically deserialize ISO-8601 strings to datatimes,
as it's impossible to distinguish them from normal strings.

However, if you use a model with a ``datetime`` field, and enable the
``isodates`` model class setting, the model will correctly convert the strings
to datetime objects (with timezone information if available):

.. sourcecode:: python

    from datetime import datetime
    import faust

    class Account(faust.Record, isodates=True, serializer='json'):
        date_joined: datetime


Subclassing models: Abstract model classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can mark a model class as ``abstract=True`` to create
a model base class, that must be subclassed to create new models
with common functionality.

For example you may want to have a base class for all models that
have creation and updated at times:

.. sourcecode:: python

    class MyBaseRecord(Record, abstract=True):
        time_created: float = None
        time_updated: float = None


This "abstract" class can not be used as a model, it can only be used to
create new models:

.. sourcecode:: python

    class Account(MyBaseRecord):
        id: str

    account = Account(id='X', time_created=3124312.3442)
    print(account.time_created)


Positional Arguments
~~~~~~~~~~~~~~~~~~~~

Models may also be instantiated using positional arguments,
so ``Point(x=10, y=30)`` may also be expressed as ``Point(10, 30)``.

The ordering of fields in positional arguments gets tricky when you
add subclasses into the mix.  In that case the ordering is decided by the method
resolution order, as demonstrated by this example:

.. sourcecode:: python

    import faust

    class Point(faust.Record):
        x: int
        y: int

    class XYZPoint(Point):
        z: int

    point = XYZPoint(10, 20, 30)
    assert (point.x, point.y, point.z) == (10, 20, 30)


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

    .. automethod:: from_data
        :noindex:

    .. automethod:: derive
        :noindex:

    .. attribute:: _options
        :noindex:

        Model metadata for introspection. An instance of
        :class:`faust.types.models.ModelOptions`.

.. class:: ModelOptions
    :noindex:

    .. autoattribute:: fields
        :noindex:

    .. autoattribute:: fieldset
        :noindex:

    .. autoattribute:: fieldpos
        :noindex:

    .. autoattribute:: optionalset
        :noindex:

    .. autoattribute:: models
        :noindex:

    .. autoattribute:: converse
        :noindex:

    .. autoattribute:: defaults
        :noindex:

.. _codecs:

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

At this point you could publish this on PyPI to share
the extension with other Faust users.
