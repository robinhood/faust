.. _guide-workers:

===============
 Workers Guide
===============

.. contents::
    :local:
    :depth: 1

.. _worker-individual:

Managing individual instances
=============================

This part describes managing individual instances and is more
relevant in development.

Make sure you also read the :ref:`worker-cluster` section of this guide
for production deployments.

.. _worker-starting:

Starting a worker
-----------------

.. sidebar:: Daemonizing

    You probably want to use a daemonization tool to start
    the worker in the background. Use `systemd`, `supervisord` or
    any of the tools you usually use to start services.
    We hope to have a detailed guide for each of these soon.

If you have defined a Faust app in the module ``proj.py``:

.. sourcecode:: python

    # proj.py
    import faust

    app = faust.App('proj', broker='kafka://localhost:9092')

    @app.agent()
    async def process(stream):
        async for value in stream:
            print(value)

You can start the worker in the foreground by executing the command:

.. code-block:: console

    $ faust -A proj worker -l info

For a full list of available command-line options simply do:

.. code-block:: console

    $ faust worker --help

You can start multiple workers for the same app on the same machine, but
be sure to provide a unique web server port to each worker, and also
a unique data directory.

Start first worker:

.. sourcecode:: console

    $ faust --datadir=/var/faust/worker1 -A proj -l info worker --web-port=6066

Then start the second worker:

.. sourcecode:: console

    $ faust --datadir=/var/faust/worker2 -A proj -l info worker --web-port=6067

.. admonition:: Sharing Data Directories

    Worker instances should not share data directories,
    so make sure to specify a different data directory for every worker
    instance.

.. _worker-stopping:

Stopping a worker
-----------------

Shutdown is accomplished using the :sig:`TERM` signal.

When shutdown is initiated the worker will finish all currently executing
tasks before it actually terminates. If these tasks are important, you should
wait for it to finish before doing anything drastic, like sending the :sig:`KILL`
signal.

If the worker won't shutdown after considerate time, for being
stuck in an infinite-loop or similar, you can use the :sig:`KILL` signal to
force terminate the worker.  The tasks that did not complete will be executed
again by another worker.

.. admonition:: Starting subprocesses

    For Faust applications that start subprocesses as a side
    effect of processing the stream, you should know that the "double-fork"
    problem on Unix means that the worker will not be able to reap its children
    when killed using the :sig:`KILL` signal.

    To kill the worker and any child processes, this command usually does
    the trick:

    .. sourcecode:: console

        $ pkill -9 -f 'faust'

    If you don't have the :command:`pkill` command on your system, you can use the slightly
    longer version:

    .. code-block:: console

        $ ps auxww | grep 'faust' | awk '{print $2}' | xargs kill -9

.. _worker-restarting:

Restarting a worker
-------------------

To restart the worker you should send the `TERM` signal and start a new
instance.

.. admonition:: Kafka Rebalancing

    When using Kafka, stopping or starting new workers will trigger a
    rebalancing operation that require all workers to stop stream processing.

    See :ref:`worker-cluster` for more information.

.. _worker-process-signals:

Process Signals
---------------

The worker's main process overrides the following signals:

+--------------+-------------------------------------------------+
| :sig:`TERM`  | Warm shutdown, wait for tasks to complete.      |
+--------------+-------------------------------------------------+
| :sig:`QUIT`  | Cold shutdown, terminate ASAP                   |
+--------------+-------------------------------------------------+
| :sig:`USR1`  | Dump traceback for all active threads in logs   |
+--------------+-------------------------------------------------+

.. _worker-cluster:

Managing a cluster
==================

In production deployments the management of a cluster of worker instances
is complicated by the Kafka rebalancing strategy.

Every time a new worker instance joins or leaves, the Kafka broker will ask
all instances to perform a "rebalance" of available partitions.

This "stop the world" process will temporarily halt processing of all streams,
and if this rebalancing operation is not managed properly, you may end up
in a state of perpetual rebalancing: the workers will continually
trigger rebalances to occur, effectively halting processing of the stream.

.. note::

    The Faust web server is not affected by rebalancing, and will
    still serve web requests.

    This is important to consider when using tables and serving
    table data over HTTP.  Tables exposed in this manner will be eventually
    consistent and may serve stale data during a rebalancing operation.

When will rebalancing occur? It will occur should you restart one of the
workers, or when restarting workers to deploy changes, and also if you
change the number of partitions for a topic to scale a cluster up or down.

Restarting a cluster
--------------------

To minimize the chance of rebalancing problems we suggest you use
the following strategy to restart all the workers:

1) Stop 50% of the workers (and wait for them to shut down).

2) Start the workers you just stopped and wait for them to fully start.

3) Stop the other half of the workers (and wait for them to shut down).

4) Start the other half of the workers.

This should both minimize rebalancing issues and also keep the built-in web
servers up and available to serve HTTP requests.

.. admonition:: KIP-441 and the future...

    The Kafka developer community have proposed a solution to this problem,
    so in the future we may have an easier way to deploy code changes
    and even support autoscaling of workers.

    See `KIP-441: Smooth Scaling Out for Kafka Streams`_
    for more information.

.. _`KIP-441: Smooth Scaling Out for Kafka Streams`:
    https://cwiki.apache.org/confluence/display/KAFKA/KIP-441%3A+Smooth+Scaling+Out+for+Kafka+Streams
