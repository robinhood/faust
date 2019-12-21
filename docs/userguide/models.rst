.. _guide-models:

=====================================
 Models, Serialization, and Codecs
=====================================

.. contents::
    :local:
    :depth: 1

.. module:: faust
    :noindex:

.. currentmodule:: faust

Basics
======

Models describe the fields of data structures used as keys and values
in messages.  They're defined using a ``NamedTuple``-like syntax::

    class Point(Record, serializer='json'):
        x: int
        y: int

Here we define a "Point" record having ``x``, and ``y`` fields of type int.

A record is a model of the dictionary type, having keys and values
of a certain type.

When using JSON as the serialization format, the Point model
serializes to:

.. sourcecode:: pycon

    >>> Point(x=10, y=100).dumps()
    {"x": 10, "y": 100}

To temporarily use a different serializer, provide that as an argument
to ``.dumps``:

.. sourcecode:: pycon

    >>> Point(x=10, y=100).dumps(serializer='pickle')  # pickle + Base64
    b'gAN9cQAoWAEAAAB4cQFLClgBAAAAeXECS2RYBwAAAF9fZmF1c3RxA31xBFg
    CAAAAbnNxBVgOAAAAX19tYWluX18uUG9pbnRxBnN1Lg=='

"Record" is the only type supported, but in the future
we also want to have arrays and other data structures.

In use
======

Models are useful when data needs to be serialized/deserialized,
or whenever you just want a quick way to define data.

In Faust we use models to:

- Describe the data used in streams (topic keys and values).
- HTTP requests (POST data).

For example here's a topic where both keys and values are points:

.. sourcecode:: python

    my_topic = faust.topic('mytopic', key_type=Point, value_type=Point)

    @app.agent(my_topic)
    async def task(events):
        async for event in events:
            print(event)

.. warning::

    Changing the type of a topic is backward incompatible change.
    You need to restart all Faust instances using the old key/value types.

    The best practice is to provide an upgrade path for old instances.

The topic already knows what type is required, so when sending
data you just provide the values as-is:

.. sourcecode:: python

    await my_topic.send(key=Point(x=10, y=20), value=Point(x=30, y=10))


.. admonition:: Anonymous Agents

    An "anonymous" agent does not use a topic description.

    Instead the agent will automatically create and manage its
    own topic under the hood.

    To define the key and value type of such an agent
    just pass them as keyword arguments:

    .. sourcecode:: python

        @app.agent(key_type=Point, value_type=Point)
        async def my_agent(events):
            async for event in events:
                print(event)

    Now instead of having a topic where we can send messages,
    we can use the agent directly:

    .. sourcecode:: python

        await my_agent.send(key=Point(x=10, y=20), value=Point(x=30, y=10))

.. _model-schemas:

Schemas
=======

A "schema" configures both key and value type for a topic,
and also the serializers used.

Schemas are also able to read the headers of Kafka messages,
and so can be used for more complex serialization support, such as
`Protocol Buffers`_ or `Apache Thrift`_.

.. _`Protocol Buffers`: https://developers.google.com/protocol-buffers/

.. _`Apache Thrift`: https://thrift.apache.org

To define a topic using a schema:

.. sourcecode:: python

    schema = faust.Schema(
        key_type=Point,
        value_type=Point,
        key_serializer='json',
        value_serializer='json',
    )

    topic = app.topic('mytopic', schema=schema)

If any of the serializer arguments are omitted, the default from the app
configuration will be used.


Schemas can also be used with "anonymous agents" (see above)

.. sourcecode:: python

    @app.agent(schema=schema)
    async def myagent(stream):
        async for value in stream:
            print(value)


Schemas are most useful when extending Faust, for example defining
a schema that reads message key and value type from Kafka headers:

.. sourcecode:: python

    import faust
    from faust.types import ModelT
    from faust.types.core import merge_headers
    from faust.models import registry

    class Autodetect(faust.Schema):

        def loads_key(self, app, message, *,
                      loads=None,
                      serializer=None):
            if loads is None:
                loads = app.serializers.loads_key
            # try to get key_type and serializer from Kafka headers
            headers = dict(message.headers)
            key_type_name = headers.get('KeyType')
            serializer = serializer or headers.get('KeySerializer')
            if key_type_name:
                key_type = registry[key_type]
                return loads(key_type, message.key,
                       serializer=serializer)
            else:
                return super().loads_key(
                    app, message, loads=loads, serializer=serializer)

        def loads_value(self, app, message, *,
                        loads=None,
                        serializer=None):
            if loads is None:
                loads = app.serializers.loads_value
            # try to get key_type and serializer from Kafka headers
            headers = dict(message.headers)
            value_type_name = headers.get('ValueType')
            serializer = serializer or headers.get('ValueSerializer')
            if value_type_name:
                value_type = registry[value_type]
                return loads(value_type, message.key,
                       serializer=serializer)
            else:
                return super().loads_value(
                    app, message, loads=loads, serializer=serializer)

        def on_dumps_key_prepare_headers(self, key, headers):
            # If key is a model, set the KeyType header to the models
            # registered name.

            if isinstance(key, ModelT):
                key_type_name = key._options.namespace
                return merge_headers(headers, {'KeyType': key_type_name})
            return headers

        def on_dumps_value_prepare_headers(self, value, headers):
            if isinstance(value, ModelT):
                value_type_name = value._options.namespace
                return merge_headers(headers, {'ValueType': value_type_name})
            return headers

        app = faust.App('id')
        my_topic = app.topic('mytopic', schema=Autodetect())


Manual Serialization
====================

Models are not required to read data from a stream.

To deserialize streams manually, use a topic with bytes values:

.. sourcecode:: python

    topic = app.topic('custom', value_type=bytes)

    @app.agent
    async def processor(stream):
        async for payload in stream:
            data = json.loads(payload)

To integrate with external systems, :ref:`codecs`
help you support serialization and de-serialization
to and from any format.  Models describe the form of messages, and codecs
explain how they're serialized, compressed, encoded, and so on.

The default codec is configured by the applications ``key_serializer`` and
``value_serializer`` arguments::

    app = faust.App(key_serializer='json')

Individual models can override the default
by specifying a ``serializer`` argument when creating the model class:

.. sourcecode:: python

    class MyRecord(Record, serializer='json'):
        ...

Codecs may also be combined to provide multiple encoding and decoding
stages, for example ``serializer='json|binary'`` will serialize as JSON
then use the Base64 encoding to prepare the payload for transmission
over textual transports.

.. seealso::

    - The :ref:`codecs` section -- for more information about codecs
      and how to define your own.

.. topic:: Sending/receiving raw values

    You don't have to use models to deserialize events in topics.
    instead you may omit the ``key_type``/``value_type`` options,
    and instead use the ``key_serializer``/``value_serializer``
    arguments:

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

    The ``raw`` serializer will provide you with raw text/bytes
    (the default is bytes, but use ``key_type=str`` to specify text):

    .. sourcecode:: python

        transfers_topic = app.topic('transfers', value_serializer='raw')

    You may also specify any other supported codec, such as json to
    use that directly:

    .. sourcecode:: python

        transfers_topic = app.topic('transfers', value_serializer='json')

Model Types
===========

Records
-------

A record is a model based on a dictionary/mapping.

Here's a simple record describing a 2d point, having two required fields:

.. sourcecode:: python

    class Point(faust.Record):
        x: int
        y: int

To create a new point, provide the fields as keyword arguments:

.. sourcecode:: pycon

    >>> point = Point(x=10, y=20)
    >>> point
    <Point: x=10, y=20>

If you forget to pass a required field, we throw an error:

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

If you don't want it to be an error, make it an optional field:

.. sourcecode:: python

    class Point(faust.Record, serializer='json'):
        x: int
        y: int = 0

You may now omit the ``y`` field when creating points:

.. sourcecode:: python

    >>> point = Point(x=10)
    <Point: x=10 y=0>

.. note::

    The order is important here: all optional fields must be
    defined **after** all requred fields.

    This is not allowed:

    .. code-block:: python

        class Point(faust.Record, serializer='json'):
            x: int
            y: int = 0
            z: int

    but this works:

    .. code-block:: python

        class Point(faust.Record, serializer='json')
            x: int
            z: int
            y: int = 0


.. _model-fields:

Fields
======

Records may have fields of arbitrary types and both
standard Python types and user defined classes will work.

Note that field types must support serialization,
otherwise we cannot reconstruct the object back
to original form.

Fields may refer to other models:

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


The field type is a type annotation,
so you can use the :pypi:`mypy` type checker to verify
arguments passed have the correct type.

We do not perform any type checking at runtime.

Collections
-----------

Fields can be collections of another type.

For example a User model may have a list of accounts:

.. sourcecode:: python

    from typing import List
    import faust


    class User(faust.Record):
        accounts: List[Account]


Not only lists are supported, you can also use dictionaries,
sets and others.

Consult this table of supported annotations:

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

From this table we can tell that we may have a *mapping*
of username to account:

.. sourcecode:: python

    from typing import Mapping
    import faust


    class User(faust.Record):
        accounts: Mapping[str, Account]

Faust will then automatically reconstruct the ``User.accounts`` field into
a mapping of account-ids to ``Account`` objects.

Coercion
--------

By default we do not force types, this is for backward compatibility
with older Faust application.

This means that a field of type :class:`str` will happily accept
:const:`None` as value, and any other type.

If you want strict types enable the ``coerce`` option:

.. sourcecode:: python

    class X(faust.Record, coerce=True):
        foo: str
        bar: Optional[str]

Here, the ``foo`` field will be required to be a string,
while the ``bar`` field can have :const:`None` values.

.. tip::

    Having ``validation=True`` implies ``coerce=True``
    but will additionally enable field validation.

    See :ref:`model-validation` for more information.

Coercion also enables automatic conversion to and from
:class:`~datetime.datetime` and :class:`~decimal.Decimal`.


You may also disable coercion for the class, but enable
it for individual fields by writing explicit field descriptors:

.. sourcecode:: python

    import faust
    from faust.models.fields import DatetimeField, StringField

    class Account(faust.Record):
        user_id: str = StringField(coerce=True)
        date_joined: datetime = DatetimeField(coerce=False)
        login_dates: List[datetime] = DatetimeField(coerce=True)

:class:`~datetime.datetime`
~~~~~~~~~~~~~~~~~~~~~~~~~~~

When using JSON we automatically convert :class:`~datetime.datetime`
fields into ISO-8601 text format, and automatically convert
back into into :class:`~datetime.datetime` when deserializing.

.. sourcecode:: python

    from datetime import datetime
    import faust

    class Account(faust.Record, coerce=True, serializer='json'):
        date_joined: datetime


Other date formats
^^^^^^^^^^^^^^^^^^

The default date parser supports ISO-8601 only.  To support
this format and many other formats (such as ``'Sat Jan 12 00:44:36 +0000 2019'``)
you can select to use :pypi:`python-dateutil` as the parser.

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

:class:`~decimal.Decimal`
~~~~~~~~~~~~~~~~~~~~~~~~~

JSON doesn't have a high precision decimal field type
so if you require high precision you must use :class:`~decimal.Decimal`.

The built-in JSON encoder will convert these to strings in the json
payload, that way we do not lose any precision.

.. sourcecode:: python

    from decimal import Decimal
    import faust

    class Order(faust.Record, coerce=True, serializer='json'):
        price: Decimal
        quantity: Decimal

Abstract Models
---------------

To create a model base class with common functionality, mark
the model class with ``abstract=True``.

Abstract models must be inherited from,
and cannot be instantiated directly.

Here's an example base class with default fields for creation
time and last modified time:

.. sourcecode:: python

    class MyBaseRecord(Record, abstract=True):
        time_created: float = None
        time_modified: float = None

Inherit from this model to create a new model
having the fields by default:

.. sourcecode:: python

    class Account(MyBaseRecord):
        id: str

    account = Account(id='X', time_created=3124312.3442)
    print(account.time_created)


Positional Arguments
--------------------

The best practice when creating model instances is to use
keyword arguments, but positional arguments are also supported!

The point ``Point(x=10, y=30)`` may also be expressed as ``Point(10, 30)``.

Back to why this is not a good practice, consider the
case of inheritance:

.. sourcecode:: python

    import faust

    class Point(faust.Record):
        x: int
        y: int

    class XYZPoint(Point):
        z: int

    point = XYZPoint(10, 20, 30)
    assert (point.x, point.y, point.z) == (10, 20, 30)


To deduce the order arguments we now have to consider the inheritance
tree, this is difficult without looking up the source code.

This quickly turns even more complicated when we add multiple
inheritance into the mix:

.. sourcecode:: python

    class Point(AModel, BModel):
        ...

We suggest using positional arguments only for simple classes
such as the Point example, where inheritance of additional fields
is not used.

Fields with the same name as a reserved keyword
-----------------------------------------------

Sometimes data you want to describe data will contan
field names that collide with a reserved Python keyword.

One such example is a field named ``in``. You cannot define
a model like this:

.. sourcecode:: python

    class OpenAPIParameter(Record):
        in: str = 'query'

doing so will result in a :exc:`NameError` exception being raised.

To properly support this, you need to rename the field
but specify an alternative ``input_name``:


.. sourcecode:: python

    from faust.models.fields import StringField

    class OpenAPIParameter(Record):
        location: str = StringField(default='query', input_name='in')

The ``input_name`` here describes the name of the field
in serialized payloads. There's also a corresponding ``output_name``
that can be used to specify what field name this field deserializes to.
The default output name is the same as the input name.

Polymorphic Fields
------------------

Felds can refer to other models, such as an account with a user field:

.. sourcecode:: python

    class User(faust.Record):
        id: str
        first_name: str
        last_name: str

    class Account(faust.Record):
        user: User
        balance: Decimal

This is a strict relationship: the value for Account.user can only
be an instance of the ``User`` type.

*Polymorphic fields* are also supported, where the
type of the field is decided at runtime.

Consider an Article models with a list of assets
where the type of asset is decided at runtime:

.. sourcecode:: python

    class Asset(faust.Record):
        url: str
        type: str

    class ImageAsset(Asset):
        type = 'image'

    class VideoAsset(Asset):
        runtime_seconds: float
        type = 'video'

    class Article(faust.Record, polymorphic_fields=True):
        assets: List[Asset]

How does this work?
Faust models add additional metadata when serialized,
just look at the payload for one of our accounts:

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

Here the metadata section is the ``__faust`` field, and
it contains the name of the model that generated this payload.

By default we don't use this name for anything at all,
but we do if polymorphic fields are enabled.

Why is it disabled by default?
There is often a mismatch between the name of the class
used to produce the event, and the class we want to reconstruct
it as.

Imagine a producer is using an outdated version,
or model cannot be shared between systems
(this happens when using different programming languages,
integrating with proprietary systems, and so on.)

The namespace ``ns`` contains the fully qualified name of the
model class (in this example ``t.User``).

Faust will keep an index of model names, and whenever you
define a new model class we add it to this index.

.. note::

    If you're trying to deserialize a model but it complains
    that it does not exist, you probably forgot to import this
    model before using it.

    For the same reason you should not be renaming classes
    without having a strategy to do so in a forward compatible manner.

.. _model-validation:

Validation
----------

For models there is no validation of data by default:
if you have a field described as an int, it will happily accept a string
or any other object that you pass to it:

.. sourcecode:: pycon

    >>> class Person(faust.Record):
    ...    age: int
    ...

    >>> p = Person(age="foo")
    >>> p.age
    "foo"

However, there is an option that will enable validation
for all common JSON fields (:class:`int`, :class:`float`, :class:`str`, etc.), and some
commonly used Python ones (:class:`~datetime.datetime`,
:class:`~decimal.Decimal`, etc.)

.. sourcecode:: pycon

    >>> class Person(faust.Record, validation=True):
    ...     age: int

    >>> p = Person(age="foo")

.. sourcecode:: pytb

    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      ValidationError: Invalid type for int field 'age': 'foo' (str)

For things like web forms raising an error automatically is not a good solution,
as the client will usually want a list of all errors.

So in web views we suggest disabling automatic validation,
and instead manually validating the model by calling ``model.validate()``.
to get a list of :class:`ValidationError` instances.

.. sourcecode:: pycon

    >>> class Person(faust.Record):
    ...     age: int
    ...     name: str

    >>> p = Person(age="Gordon Gekko", name="32")
    >>> p.validate()
    [
      ('age': ValidationError(
            "Invalid type for int field 'age': 'Gordon Gekko' (str)"),
      ('name': ValidationError(
            "Invalid type for str field 'name': 32 (int)")),
    ]

Advanced Validation
~~~~~~~~~~~~~~~~~~~

If you have a field you want validation for,
you may explicitly define the field descriptor for the field you want
validation on (note: this will override the built-in
validation for that field). This will also enable you to access
more validation options, such as the maximum number of characters
for a string, or a minmum value for an integer:

.. sourcecode:: python

    class Person(faust.Record, validation=True):
        age: int = IntegerField(min_value=18, max_value=99)
        name: str


Custom field types
~~~~~~~~~~~~~~~~~~

You may define a custom :class:`~faust.models.FieldDescriptor` subclass
to perform your own validation:

.. sourcecode:: python

    from typing import Any, Iterable, List
    from faust.exceptions import ValidationError
    from faust.models import FieldDescriptor

    class ChoiceField(FieldDescriptor[str]):

        def __init__(self, choices: List[str], **kwargs: Any) -> None:
            self.choices = choices
            # Must pass any custom args to init,
            # so we pass the choices keyword argument also here.
            super().__init__(choices=choices, **kwargs)

        def validate(self, value: str) -> Iterable[ValidationError]:
            if value not in self.choices:
                choices = ', '.join(self.choices)
                yield self.validation_error(
                    f'{self.field} must be one of {choices}')


After defining the subclass you may use it to define model fields:

.. sourcecode:: pycon

    >>> class Order(faust.Record):
    ...    side: str = ChoiceField(['SELL', 'BUY'])

    >>> Order(side='LEFT')
    faust.exceptions.ValidationError: (
        'side must be one of SELL, BUY', <ChoiceField: Order.side: str>)

.. _model-field-exclude:

Excluding fields from representation
------------------------------------

If you want your model to accept a certain field when deserializing,
but exclude the same field from serialization, you can do so
by marking that field as ``exclude=True``:

.. sourcecode:: python

    import faust
    from faust.models.fields import StringField


    class Order(faust.Record):
        price: float
        quantity: float
        user_id: str = StringField(required=True, exclude=True)


This model will accept ``user_id`` as a keyword argument, and from any
serialized structure:

.. sourcecode:: pycon

    >>> order = Order(price=30.0, quantity=2.0, user_id='foo')
    >>> order.user_id
    'foo'

    >>> order2 = Order.loads(
    ...     '{"price": "30.0", quantity="2.0", "user_id": "foo"}',
    ...     serializer='json',
    ... )

    >>> order2.user_id
    'foo'

But when serializing the order, the field will be excluded:

.. sourcecode:: pycon

    >>> order.asdict()
    {'price': 30.0, 'quantity': 2.0}

    >>> order.dumps(serializer='json')
    '{"price": "30.0", "quantity": "2.0"}'


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
to raw msgpack format.

You may also combine the Base64 codec to support transports unable
to handle binary data (such as HTTP or Redis):

Combining codecs is done using the ``|`` operator:

.. sourcecode:: python

    def msgpack() -> codecs.Codec:
        return raw_msgpack() | codecs.binary()

    codecs.register('msgpack', msgpack())

.. sourcecode:: pycon

    >>> import my_msgpack_codec

    >>> from faust import Record
    >>> class Point(Record, serializer='msgpack'):
    ...     x: int
    ...     y: int


At this point we have to import the codec every time
we want to use it, that is very cumbersome.

Faust also supports registering *codec extensions*
using :pypi:`setuptools` entry-points, so instead lets create an installable
msgpack extension!

Define a package with the following directory layout:

.. sourcecode:: text

    faust-msgpack/
        setup.py
        faust_msgpack.py

The first file (:file:`faust-msgpack/setup.py`) defines metadata about our
package and should look like:

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

The most important part here is the ``entry_points`` section
that tells setuptools how to load our plugin.

We have set the name of our
codec to ``msgpack`` and the path to the codec class
to be ``faust_msgpack:msgpack``.

Faust imports this as it would do
``from faust_msgpack import msgpack``, so we need to define
hat part next in our :file:`faust-msgpack/faust_msgpack.py` module:

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
with other Faust users.
