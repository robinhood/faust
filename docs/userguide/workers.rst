.. _guide-workers:

===============
 Workers Guide
===============

.. contents::
    :local:
    :depth: 1

.. _worker-starting:

Starting the worker
===================

.. sidebar:: Daemonizing

    You probably want to use a daemonization tool to start
    the worker in the background. Use systemd, supervisord or
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

.. _worker-stopping:

Stopping the worker
===================

Shutdown should be accomplished using the :sig:`TERM` signal.

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
    effect of processsing the stream, you should know that the "double-fork"
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

Restarting the worker
=====================

To restart the worker you should send the `TERM` signal and start a new
instance.

.. _worker-process-signals:

Process Signals
===============

The worker's main process overrides the following signals:

+--------------+-------------------------------------------------+
| :sig:`TERM`  | Warm shutdown, wait for tasks to complete.      |
+--------------+-------------------------------------------------+
| :sig:`QUIT`  | Cold shutdown, terminate ASAP                   |
+--------------+-------------------------------------------------+
| :sig:`USR1`  | Dump traceback for all active threads.          |
+--------------+-------------------------------------------------+
