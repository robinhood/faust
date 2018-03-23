.. _quickstart:

============================================================
  Quickstart
============================================================

.. contents::
    :local:
    :depth: 2

Hello World
===========

Application
-----------

The first thing you need to get up and running with Faust is to define
an application.

The application (or app for short) configures your project and implements
common functionality. We also define a topic description, and an agent
to process messages in that topic.

Lets create the file `hello_world.py`:

.. sourcecode:: python

    import faust

    app = faust.App(
        'hello-world',
        broker='kafka://localhost:9092',
        value_serializer='raw',
    )

    greetings_topic = app.topic('greetings')

    @app.agent(greetings_topic)
    async def greet(greetings):
        async for greeting in greetings:
            print(greeting)

In this tutorial, we keep everything in a single module, but for larger
projects, you can create a dedicated package with a submodule layout.

The first argument passed to the app is the ``id`` of the application, needed
for internal bookkeeping and to distribute work among worker instances.

By default Faust will use JSON serialization, so we specify ``value_serializer``
here as ``raw`` to avoid deserializing incoming greetings.  For real
applications you should define models (see :ref:`guide-models`).

Here you defined a Kafka topic ``greetings`` and then iterated over the
messages in the topic and printed each one of them.

.. note::

    The application :setting:`id` setting (i.e. ``'hello-world'`` in
    the example above), should be unique per Faust app in your Kafka
    cluster.


Starting Kafka
--------------

Before running your app, you need to start Zookeeper and Kafka.

Start Zookeeper first:

.. sourcecode:: console

    $ $KAFKA_HOME/bin/zookeeper-server-start $KAFKA_HOME/etc/kafka/zookeeper.properties

Then start Kafka:

.. sourcecode:: console

    $ $KAFKA_HOME/bin/kafka-server-start $KAFKA_HOME/etc/kafka/server.properties

Running the Faust worker
------------------------

Now that you have created a simple Faust application and have Kafka and
Zookeeper running, you need to run a worker instance for the application.

Start a worker:

.. sourcecode:: console

    $ faust -A hello_world worker -l info

Multiple instances of a Faust worker can be started independently to distribute
stream processing across machines and CPU cores.

In production, you'll want to run the worker in the
background as a daemon. Use the tools provided
by your platform, or use something like `supervisord`_.

Use ``--help`` to get a complete listing of available command-line options:

.. sourcecode:: console

    $ faust worker --help

.. _`supervisord`: http://supervisord.org

Seeing things in Action
-----------------------

At this point, you have an application running, but not much is happening.
You need to feed data into the Kafka topic to see Faust print the greetings
as it processes the stream, and right now that topic is probably empty.

Let's use the :program:`faust send` command to push some messages into the
``greetings`` topic:

.. sourcecode:: console

    $ faust -A hello_world send @greet "Hello Faust"

The above command sends a message to the ``greet`` agent by using the ``@``
prefix. If you don't use the prefix, it will be treated as the name of a topic:

.. sourcecode:: console

    $ faust -A hello_world send greetings "Hello Kafka topic"

After sending the messages, you can see your worker start processing them
and print the greetings to the console.

Where to go from here...
------------------------

Now that you have seen a simple Faust application in action,
you should dive into the other sections of the :ref:`guide` or jump right
into the :ref:`playbooks` for tutorials and solutions to common patterns.
