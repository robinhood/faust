.. _guide-models:

=====================================
 Models, Serialization, and Codecs
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

Here we define a "Point" record having ``x``, and ``y`` fields of type int.
There’s no type checking at runtime, but the :pypi:`mypy` type checker can be used
as a separate build step to verify that arguments have the correct type.

A record is a model of the dictionary type, and describes keys and values.
When using JSON as the serialization format, the Point model above
serializes as:

.. sourcecode:: pycon

    >>> Point(x=10, y=100).dumps()
    {"x": 10, "y": 100}

A different serializer can be provided as an argument to ``.dumps``:

.. sourcecode:: pycon

    >>> Point(x=10, y=100).dumps('pickle')  # pickle + Base64
    b'gAN9cQAoWAEAAAB4cQFLClgBAAAAeXECS2RYBwAAAF9fZmF1c3RxA31xBFg
    CAAAAbnNxBVgOAAAAX19tYWluX18uUG9pbnRxBnN1Lg=='

"Record" is the only model type supported by this version of Faust,
but is just one of many possible model types to include in the future.
The Avro serialization schema format, which the terminology is taken from,
supports records, arrays, and more.

Manual Serialization
====================

You're not required to define models to read the data from a stream.
Manual de-serialization also works and is rather easy to perform.
Models provide additional benefit, such as the field descriptors that
let you refer to fields in `group_by` statements, static typing using
:pypi:`mypi`, automatic conversion of :class:`datetime`, and so on...

To deserialize streams manually, merely use a topic with bytes values:

.. sourcecode:: python

    topic = app.topic('custom', value_type=bytes)

    @app.agent
    async def processor(stream):
        async for payload in stream:
            data = json.loads(payload)

To integrate with external systems, Faust's :ref:`codecs`
can help you support serialization and de-serialization
to and from any format.  Models describe the form of messages, while codecs
explain how they're serialized/compressed/encoded/etc.

The default codec is configured by the applications ``key_serializer`` and
``value_serializer`` arguments::

    app = faust.App(key_serializer='json')

Individual models can override the default
by specifying a ``serializer`` argument when creating the model class:

.. sourcecode:: python

    class MyRecord(Record, serializer='json'):
        ...

Codecs can also be combined, so they consist of multiple encoding and decoding
stages, for example, data serialized with JSON and then Base64 encoded would
be described as the keyword argument ``serializer='json|binary'``.

.. seealso::

    - The :ref:`codecs` section -- for more information about codecs
      and how to define your own.

.. topic:: Sending/receiving raw values

    Serializing/deserializing keys and values manually without models is easy.
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

Using models to describe topics provides benefits:

.. sourcecode:: python

    # examples/described.py
    import faust

    class Transfer(faust.Record):
        account_id: str
        amount: float

    app = faust.App('values')
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

The :pypi:`mypy` static type analyzer can now alert you if your
code is passing the wrong type of value for the ``account_id`` field,
and more.  The most compelling reason for using non-described messages would
be to integrate with existing Kafka topics and systems, but if you're
writing new systems in Faust, the best practice would be to describe models
for your message data.

Model Types
===========

The first version of Faust only supports dictionary models (records),
but can be easily extended to support other types of models, like arrays.

Records
-------

A record is a model based on a dictionary/mapping.  The storage
used is a dictionary, and it serializes to a dictionary, but the same
is true for ordinary Python objects and their ``__dict__`` storage, so you can
consider record models to be "objects" that can have methods and properties.

Here's a simple record describing a 2d point, with two required fields: ``x``
and ``y``:

.. sourcecode:: python

    class Point(faust.Record):
        x: int
        y: int

To create a new point, instantiate it like a regular Python object,
and provide fields as keyword arguments:

.. sourcecode:: pycon

    >>> point = Point(x=10, y=20)
    >>> point
    <Point: x=10, y=20>

Faust throws an error if you instantiate a model without providing
values for all required fields:

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

    Python does not check types at runtime.
    The annotations are only used by static analysis tools like :pypi:`mypy`.

To describe an optional field, provide a default value:

.. sourcecode:: python

    class Point(faust.Record, serializer='json'):
        x: int
        y: int = 0

You can now omit the ``y`` field when creating a new point:

.. sourcecode:: pycon

    >>> point = Point(x=10)
    >>> point
    <Point: x=10, y=0>

When sending messages to topics, we can use ``Point`` objects as message keys,
and values:

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

.. warning::

    You need to restart all Faust instances using the old key/value types,
    or alternatively provide an upgrade path for old instances.

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

.. sourcecode:: pycon

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

This works with many of the iterable types, so for a list all
of :class:`~typing.Sequence`, :class:`~typing.MutableSequence`, and :class:`~typing.List`
can be used. For a full list of generic data types
recognized by Faust, consult the following table:

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


From this table we can see that we can also have a *mapping*
of username to account:

.. sourcecode:: python

    from typing import Mapping
    class DOA(faust.Record):
        accounts: Mapping[str, Account]

Faust will automatically reconstruct the ``DOA.accounts`` field into
a mapping of string to ``Account`` objects.


There are limitations to this, and Faust may not recognize your custom
mapping or list type, so stick to what is listed in the table for your
Faust version.


Coercion
~~~~~~~~

Automatic coercion of datetimes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Faust automatically serializes :class:`~datetime.datetime` fields to
ISO-8601 text format but will not automatically deserialize ISO-8601 strings
back into :class:`~datetime.datetime` (it is impossible to distinguish them
from ordinary strings).

However, if you use a model with a :class:`~datetime.datetime` field, and enable the
``isodates`` model class setting, the model will correctly convert the strings
to datetime objects (with timezone information if available) when
deserialized:

.. sourcecode:: python

    from datetime import datetime
    import faust

    class Account(faust.Record, isodates=True, serializer='json'):
        date_joined: datetime

Automatic coercion of decimals
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Similar to datetimes, json does not have a suitable high precision
decimal field type.

You can enable the ``decimals=True`` option to coerce string decimal
values back into Python :class:`decimal.Decimal` objects.

.. sourcecode:: python

    from decimal import Decimal
    import faust

    class Order(faust.Record, decimals=True, serializer='json'):
        price: Decimal
        quantity: Decimal


Custom coercions
^^^^^^^^^^^^^^^^

You can add custom coercion rules to your model classes
using the ``coercions`` options.  This must be a mapping from, either a tuple
of types or a single type, to a function/class/callable used to convert it.

Here's an example converting strings back to UUID objects:

.. sourcecode:: python

    from uuid import UUID
    import faust

    class Account(faust.Record, coercions={UUID: UUID}):
        id: UUID

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

Subclassing models: Abstract model classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can mark a model class as ``abstract=True`` to create a model base class,
that you must inherit from to create new models having common functionality.

For example, you may want to have a base class for all models that
have fields for time of creation, and time last created.

.. sourcecode:: python

    class MyBaseRecord(Record, abstract=True):
        time_created: float = None
        time_updated: float = None

An “abstract” class is only used to create new models:

.. sourcecode:: python

    class Account(MyBaseRecord):
        id: str

    account = Account(id='X', time_created=3124312.3442)
    print(account.time_created)


Positional Arguments
~~~~~~~~~~~~~~~~~~~~

You can also create model values using positional arguments,
meaning that ``Point(x=10, y=30)`` can also be expressed as ``Point(10, 30)``.

The ordering of fields in positional arguments gets tricky when you
add subclasses to the mix.  In that case, the ordering is decided by the method
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


Blessed Keys and polymorphic fields
-----------------------------------

Models can contain fields that are other models, such as in this example
where an account has a user:

.. sourcecode:: python

    class User(faust.Record):
        id: str
        first_name: str
        last_name: str

    class Account(faust.Record, decimals=True):
        user: User
        balance: Decimal

This is a strict relationship, the value for Account.user can only
ever be a ``User`` class.

Faust records also support polymorphic fields, where the type of
the field is decided at runtime. Consider an Article model having
a list of assets:

.. sourcecode:: python


    class Asset(faust.Record):
        url: str
        type: str


    class ImageAsset(faust.Record):
        type = 'image'

    class VideoAsset(faust.Record):
        runtime_seconds: float
        type = 'video'

    class Article(faust.Record, allow_blessed_key=True):
        assets: List[Asset]


How does this work? What is a *blessed key*?
The answer is in how Faust models are serialized and deserialized.

When serializing a Faust model we always add a special key,
let's look at the Account object we defined above,
and how the payloads are generated:

.. sourcecode:: pycon

    >>> user = User(
    ...    id='07ecaebf-48c4-4c9e-92ad-d16d2f4a9a19',
    ...    first_name='Franz',
    ...    last_name='Kafka',
    ... )
    >>> account = Account(
    ...    user=user,
    ...    balance='12.3',
    )
    >>> from pprint import pprint
    >>> pprint(account.to_representation())
    {
        '__faust': {'ns': 't.Account'},
        'balance': Decimal('12.3'),
        'user': {
            '__faust': {'ns': 't.User'},
            'first_name': 'Franz',
            'id': '07ecaebf-48c4-4c9e-92ad-d16d2f4a9a19',
            'last_name': 'Kafka',
        },
    }


The *blessed key* here is the ``__faust`` key, it describes what model
class was used when serializing it.  When we allow the blessed key to be used,
we allow it to be reconstructed using that same class.

When you define a module in Python code, Faust will automatically
keep an index of model name to class, which we then use to look up a model
class by name.  For this to work, you must have imported the module where your
model is defined before you deserialize the payload.

When using blessed keys it's extremely important that you do not rename
classes, or old data cannot be deserialized.

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

    .. autoattribute:: decimals
        :noindex:

    .. autoattribute:: isodates
        :noindex:

    .. autoattribute:: coercions
        :noindex:

    .. autoattribute:: defaults
        :noindex:

.. _codecs:

Codecs
======

Supported codecs
----------------

* **raw**     - no encoding/serialization (bytes only).
* **json**    - :mod:`json` with UTF-8 encoding.
* **pickle**  - :mod:`pickle` with Base64 encoding (not URL-safe).
* **binary**  - Base64 encoding (not URL-safe).

Encodings are not URL-safe if the encoded payload cannot be embedded
directly into a URL query parameter.

Serialization by name
---------------------

The :func:`dumps` function takes a codec name and the object to encode as arguments,
and returns bytes

.. sourcecode:: pycon

    >>> s = dumps('json', obj)

In reverse direction, the :func:`loads` function takes a codec name and
an encoded payload to decode (in bytes), as arguments, and returns a
reconstruction of the serialized object:

.. sourcecode:: pycon

    >>> obj = loads('json', s)

When passing in the codec type as a string (as in ``loads('json', ...)`` above), you can also
combine multiple codecs to form a pipeline, for example ``"json|gzip"`` combines JSON
serialization with gzip compression:

.. sourcecode:: pycon

    >>> obj = loads('json|gzip', s)

Codec registry
--------------

All codecs have a name and the :attr:`faust.serializers.codecs` attribute
maintains a mapping from name to :class:`Codec` instance.

You can add a new codec to this mapping by executing:

.. sourcecode:: pycon

    >>> from faust.serializers import codecs
    >>> codecs.register(custom, custom_serializer())

To create a new codec, you need to define only two methods: first
you need the ``_loads()`` method to deserialize bytes, then you need
the ``_dumps()`` method to serialize an object:

.. sourcecode:: python

    import msgpack

    from faust.serializers import codecs

    class raw_msgpack(codecs.Codec):

        def _dumps(self, obj: Any) -> bytes:
            return msgpack.dumps(obj)

        def _loads(self, s: bytes) -> Any:
            return msgpack.loads(s)

We use ``msgpack.dumps`` to serialize, and our codec now encodes
to raw msgpack format in binary. We may have to write
this payload to somewhere unable to handle binary data well,
to solve that we combine the codec with Base64 encoding to convert
the binary to text.

Combining codecs is easy using the ``|`` operator:

.. sourcecode:: python

    def msgpack() -> codecs.Codec:
        return raw_msgpack() | codecs.binary()

    codecs.register('msgpack', msgpack())

At this point, we monkey-patched Faust to support
our codec, and we can use it to define records:

.. sourcecode:: pycon

    >>> from faust import Record
    >>> class Point(Record, serializer='msgpack'):
    ...     x: int
    ...     y: int

The problem with monkey-patching is that we must make sure the patching
happens before we use the feature.

Faust also supports registering *codec extensions*
using :pypi:`setuptools` entry-points, so instead, we can create an installable
msgpack extension.

To do so, we need to define a package with the following directory layout:

.. sourcecode:: text

    faust-msgpack/
        setup.py
        faust_msgpack.py

The first file (:file:`faust-msgpack/setup.py`) defines metadata about our
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
to be ``faust_msgpack:msgpack``. Faust imports this as it would
``from faust_msgpack import msgpack``, so we need to define
that part next in our :file:`faust-msgpack/faust_msgpack.py` module:

.. sourcecode:: python

    from faust.serializers import codecs

    class raw_msgpack(codecs.Codec):

        def _dumps(self, obj: Any) -> bytes:
            return msgpack.dumps(s)


    def msgpack() -> codecs.Codec:
        return raw_msgpack() | codecs.binary()

That's it! To install and use our new extension do:

.. sourcecode:: console

    $ python setup.py install

At this point you can publish this to PyPI so it can be shared
amongst other Faust users.
