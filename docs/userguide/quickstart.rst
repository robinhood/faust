.. _guide-quickstart:

============================================================
  Quick Start
============================================================

.. contents::
    :local:
    :depth: 2

Hello World
===========

Application
-----------

The first thing you need to get up and running with Faust is to define an
Application or simple a Faust app. Multiple instances of a Faust application
can be started independently to distribute the stream processing.

In this tutorial we will keep everything in a single module, but for larger
projects you may want to create a dedicated module.

Lets create the file `hello_world.py`:

.. sourcecode:: python

    import faust

    app = faust.App(
        'hello-world',
        url='kafka://localhost:9092',
        value_serializer='raw',
    )

    greetings_topic = app.topic('greetings')

    @app.actor(greetings_topic)
    async def greet(greetings):
        async for greeting in greetings:
            print(greeting)


The first argument to ``faust.App`` is the ``id`` of the application. This is
needed for internal bookkeeping for the application and to distribute work
among different instances of the application.

We specify ``value_serializer`` here as ``raw`` to avoid deserializing
incoming ``greetings``. The default ``value_serializer`` is ``json`` as we
typically would serialize/deserialize messages into well-defined models. See
:doc:`models`.

Here you defined a Kafka topic ``greetings`` and then iterated over the
messages in the topic and printed each one of them.

.. note::

    The ``App.id`` i.e. ``'hello-world'`` in the example above, should be
    unique per Faust app in your kafka cluster (or whatever message broker
    you use).


Starting Kafka
--------------

You first need to start Kafka before running your first app that you wrote
above.

For Kafka, you first need to start Zookeeper:

.. sourcecode:: console

    $ $KAFKA_HOME/bin/zookeeper-server-start $KAFKA_HOME/etc/kafka/zookeeper.properties

Next, start Kafka:

.. sourcecode:: console

    $ $KAFKA_HOME/bin/kafka-server-start $KAFKA_HOME/etc/kafka/server.properties


Running the Faust worker
------------------------

Now that you have created a simple Faust application and have kafka running,
you need to run an instance of the application. This can be done as follows:

.. sourcecode:: console

    $ faust -A hello_world worker -l info


In production you'll want to run the worker in the
background as a daemon. To do this you need to use the tools provided
by your platform, or something like `supervisord`_.

For a complete listing of the command-line options available, do:

.. sourcecode:: console

    $ faust worker --help

.. _`supervisord`: http://supervisord.org

Seeing things in Action
-----------------------

At this point you may have an application running but nothing much is
happening. You need to feed in data into the Kafka topic defined above to see
Faust print the greetings as it processes the stream. Let us use the Kafka
console producer to push some messages into the ``greetings`` topic:

.. sourcecode:: console

    $ faust -A hello_world send @greet "Hello Faust"

The above command sends a message to the ``greet`` actor by using the ``@``
prefix.  You can also send it to the topic by not using any prefix:

.. sourcecode:: console

    $ faust -A hello_world send greetings "Hello Kafka topic"

After sending these messages you can see your worker start processing
these greetings as they come in and print them.

Where to go from here
---------------------

Now that you have tried out a basic Faust application in action, you may dive
into other sections of the :ref:`guide` or jump right into the :ref:`playbooks`
which are a coolection of example use cases and patterns.
