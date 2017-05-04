.. _guide-models:

========================================================
 Models and Serialization
========================================================

.. contents::
    :local:
    :depth: 1

.. module:: faust

.. currentmodule:: faust

Basics
======

Models describe how message keys and values are serialized and deserialized.

Records
-------

A record is a model based on a dictionary/mapping and is probably the most
commonly used model.

Here's a simple record describing a 2d point, with two required fields: ``x``
and ``y``:

.. code-block:: python

    class Point(faust.Record):
        x: int
        y: int

To create a new point, instantiate it like a regular Python object.  The
fields are provided as keyword arguments:

.. code-block:: pycon

    >>> point = Point(x=10, y=20)
    >>> point
    <Point: x=10, y=20>

Attempting to instantiate a model without providing a value for a required
field is an error:

.. code-block:: pycon

    >>> point = Point(x=10)

.. code-block:: pytb

    Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
    File "/opt/devel/faust/faust/models/record.py", line 96, in __init__
        self._init_fields(fields, using_args=False)
    File "/opt/devel/faust/faust/models/record.py", line 106, in _init_fields
        type(self).__name__, ', '.join(sorted(missing))))
    TypeError: Point missing required arguments: y

.. note::

    The annotations are used for static analysis only, there's no type checking
    performed at runtime.

To describe an optional field, simply set a default value:

.. code-block:: python

    class Point(faust.Record, serializer='json'):
        x: int
        y: int = 0

You can now omit the ``y`` field when creating a new point:

.. code-block:: pycon

    >>> point = Point(x=10)
    >>> point
    <Point: x=10, y=0>

We can now use ``Point`` objects as message keys, and message values:

.. code-block:: python

    await app.send('mytopic', key=Point(x=10, y=20), value=Point(x=30, y=10))

The above will send a message to Kafka, to have a stream automatically
deserialize these points, use ``faust.topic`` to describe a topic as having
points as key and value types:

.. code-block:: python

    my_topic = faust.topic('mytopic', key_type=Point, value_type=Point)

    @app.task
    async def task(app):
        async for event in app.stream(my_topic):
            print(event)

Records can also have other records as fields:

.. code-block:: python

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

.. code-block:: python

    >>> json = transfer.dumps()

To convert the JSON back into a model use the ``.loads()`` class method:

.. code-block:: pycon

    >>> transfer = Transfer.loads(json)

Codecs
======

Supported codecs
----------------

* **json**    - json with utf-8 encoding.
* **pickle**  - pickle with base64 encoding (not urlsafe)
* **binary**  - base64 encoding (not urlsafe)

Serialization by name
---------------------

The func:`dumps` function takes a codec name and the object to encode,
the return value is bytes:

.. code-block:: pycon

    >>> s = dumps('json', obj)

For the reverse direction, the func:`loads` function takes a codec
name and a encoded payload to decode (bytes):

.. code-block:: pycon

    >>> obj = loads('json', s)

You can also combine encoders in the name, like in this case
where json is combined with gzip compression:

.. code-block:: pycon

    >>> obj = loads('json|gzip', s)

Codec registry
--------------

Codecs are configured by name and the :mod:`faust.serializers.codecs` module
maintains a mapping from name to :class:`Codec` instance: the :attr:`codecs`
attribute.

You can add a new codec to this mapping by:

.. code-block:: pycon

    >>> from faust.serializers import codecs
    >>> codecs.register(custom, custom_serializer())

A codec subclass requires two methods to be implemented: ``_loads()``
and ``_dumps()``:

.. code-block:: python

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

.. code-block:: python

    def msgpack() -> codecs.Codec:
        return raw_msgpack() | codecs.binary()

    codecs.register('msgpack', msgpack())

At this point we monkey-patched Faust to support
our codec, and we can use it to define records:

.. code-block:: pycon

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

.. code-block:: text

    faust-msgpack/
        setup.py
        faust_msgpack.py

The first file, :file:`faust-msgpack/setup.py`, defines metadata about our
package and should look like the following example:

.. code-block:: python

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

.. code-block:: python

    from faust.serializers import codecs

    class raw_msgpack(codecs.Codec):

        def _dumps(self, obj: Any) -> bytes:
            return msgpack.dumps(s)


    def msgpack() -> codecs.Codec:
        return raw_msgpack() | codecs.binary()

That's it! To install and use our new extension we do:

.. code-block:: console

    $ python setup.py install

At this point may want to publish this on PyPI to share
the extension with other Faust users.
