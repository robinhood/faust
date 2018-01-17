.. _playbooks-leader-election:

============================================================
  Tutorial: Leader Election
============================================================

.. contents::
    :local:
    :depth: 2

Faust processes streams of data forming pipelines. Sometimes steps in
the pipeline require synchronization, but instead of using mutexes,
a better solution is to have one of the workers elected as the leader.

An example of such an application is a news crawler. We can elect one
of the workers to be the leader, and the leader maintains
all subscriptions (the sources to crawl), then periodically tells the
other workers in the cluster to process them.

To demonstrate this we implement a straightforward example where we
elect one of our workers as the leader. This leader then periodically
send out random greetings to be printed out by available workers.

Application
-----------

As we did in the :ref:`playbooks-pageviews` tutorial, we first define your
application.

Create a module named :file:`leader.py`:

.. sourcecode:: python

    # examples/leader.py

    import faust

    app = faust.App(
        'leader-example',
        broker='kafka://localhost:9092',
        value_serializer='raw',
    )

Greetings Agent
---------------

Next we define the ``say`` :class:`~@App.agent` that will get greetings from the
leader and print them out to the console.

Create the agent:

.. sourcecode:: python

    @app.agent()
    async def say(greetings):
        async for greeting in greetings:
            print(greeting)

.. seealso::

    - The :ref:`guide-agents` guide -- for more information about agents.

Leader Timer
------------

Now define a :class:`~@App.timer` with the ``on_leader`` flag enabled
so it only executes on the leader.

The ``timer`` will periodically send out a random greeting, to be printed
by one of the workers in the cluster.


Create the leader timer:

.. sourcecode:: python

    import random

    @app.timer(2.0, on_leader=True)
    async def publish_greetings():
        print('PUBLISHING ON LEADER!')
        greeting = str(random.random())
        await say.send(value=greeting)

.. note::

    The greeting could be picked up by the agent ``say`` on any one of the
    running instances.

Starting Kafka
--------------

To run the project you first need to start Zookeeper and Kafka.

Start Zookeeper:

.. sourcecode:: console

    $ $KAFKA_HOME/bin/zookeeper-server-start $KAFKA_HOME/etc/kafka/zookeeper.properties

Then start Kafka:

.. sourcecode:: console

    $ $KAFKA_HOME/bin/kafka-server-start $KAFKA_HOME/etc/kafka/server.properties


Starting the Faust worker
-------------------------

Start the Faust worker, similarly to how we do it in the :ref:`quickstart`
tutorial:

.. sourcecode:: console

    $ faust -A leader worker -l info --web-port 6066

Let's start two more workers in different terminals on the same machine:

.. sourcecode:: console

    $ faust -A leader worker -l info --web-port 6067

.. sourcecode:: console

    $ faust -A leader worker -l info --web-port 6068

Seeing things in Action
-----------------------

Next try to arbitrary shut down (:kbd:`Control-c`) some of the workers,
to see how the leader stays at just *one* worker - electing a new leader
upon killing a leader -- and to see the greetings printed by the workers.
