.. _playbooks-leader-election:

============================================================
  Leader Election
============================================================

.. contents::
    :local:
    :depth: 2

Faust can be used for long running applications that need to distribute some
periodic work across a cluster of machines. A common pattern for such
applications is to elect one of the workers as a Leader, which distributes
the periodic task across the cluster of machines.

An example of such an application is a news crawler. We can elect one of the
workers to be the leader, which queues up the different sources to crawl. Then
the different queued up sources could be crawled by the cluster of machines
in parallel.

In this playbook we will go over a very simple example in which we will elect
one of our workers as the leader. This leader will then periodically send out
random greetings to be printed out by the available workers.

Application
-----------

As we did in the :ref:`_pageviews`, we first define our application.
Let us create a module ``leader.py`` and define the application:

.. sourcecode:: python
    # examples/leader.py

    import faust

    app = faust.App(
        'leader-example',
        url='kafka://localhost:9092',
        value_serializer='raw',
    )

Greetings Agent
---------------

We first define an :class:`~@App.agent` that will get the greetings from the
leader and print it out to the console.

.. sourcecode:: python

    @app.agent()
    async def say(greetings):
        async for greeting in greetings:
            print(greeting)

Here we have defined an ``agent`` to which we can send greetings
which would be printed to the console.

.. seealso::

    The :ref:`agents-guide` guide for more information about agents.

Leader Timer
------------

Let us now define the :class:`~@App.timer` that want to run only on the leader.
This ``timer`` will periodically send out a random greeting to be printed on
one of the workers in the cluster.

.. sourcecode:: python

    import random

    @app.timer(2.0, on_leader=True)
    async def publish_greetings():
        print('PUBLISHING ON LEADER!')
        greeting = str(random.random())
        await say.send(value=greeting)

Here we send a random greeting to the ``agent`` defined above.

The ``on_leader=True`` ensures that the ``timer``

.. note::

    The greeting could be picked up by the agent ``say`` on any one of the
    running instances.

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

As in the :ref:`guide-quickstart` start the application as follows:

.. sourcecode:: console

    $ faust -A leader worker -l info --web-port 6066

Let us start 2 more workers in different processes

.. sourcecode:: console

    $ faust -A leader worker -l info --web-port 6067

.. sourcecode:: console

    $ faust -A leader worker -l info --web-port 6068

Seeing things in Action
-----------------------

Now try to arbitrary kill (ctl + C) the different works to see how the leader
stays at just *one* worker - electing a new leader upon killing a leader - and
how the greetings are randomly printed across the available workers.
