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

    app = faust.App('hello-world', url='kafka://localhost:9092')

    greetings_topic = app.topic('greetings')

    async for greeting in app.stream(greetings_topic):
        print(greeting)


The first argument to ``faust.App`` is the name of the application. This is
needed for internal bookkeeping for the application and to distribute work
among different instances of the application.

Here you defined a Kafka topic ``greetings`` and then iterate over the
messages in the topic and print each one of them.

Running the Faust worker
------------------------

Now that you have created a simple Faust application, you need to run an
instance of the application. This can be done as follows:

.. sourcecode:: console

    $ faust -A hello_world worker -l info


In production you'll want to run the worker in the
background as a daemon. To do this you need to use the tools provided
by your platform, or something like `supervisord`_.

For a complete listing of the command-line options available, do:

.. sourcecode:: console

    $ faust worker --help

There are also several other commands available, and help is also available:

.. sourcecode:: console

    $ faust help

.. _`supervisord`: http://supervisord.org

Seeing things in Action
-----------------------

At this point you may have an application running but nothing much is
happening. You need to feed in data into the Kafka topic defined above to see
Faust print the messages as it processes the stream. Let us use the Kafka
console producer to push some messages into the ``greetings`` topic:

.. sourcecode:: console

    $ $KAFKA_HOME/bin/kafka-console-producer --broker-list localhost:9092 --topic greetings

The above command starts a Kafka producer. Now start sending it messages and
see your application start processing the greetings as they come in and print
them.
