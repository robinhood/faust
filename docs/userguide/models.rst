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


