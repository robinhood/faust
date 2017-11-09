.. _guide-tasks:

============================================
 Tasks, Timers, Web Views, and CLI Commands
============================================

.. contents::
    :local:
    :depth: 1

.. module:: faust

.. currentmodule:: faust

.. _task-basics:

Tasks
=====

Your application will have agents that process events in streams, but
can also start asyncio.Tasks that do other things, like periodic timers,
views for the embedded web server, or command-line commands.

Decorating an async function with the `@app.task` decorator will
tell the worker to start that function as soon as the worker is fully
operational:

.. sourcecode:: python

    @app.task
    async def on_started():
        print('APP STARTED')

If you add the above to the module that defines your app and start the worker,
you should see the message printed after starting the worker.

A task is a one-off task, if you want to do something at periodic intervals
you can use a timer.

.. _tasks-timers:

Timers
======

A timer is also a task, but one that executes every ``n`` seconds:

.. sourcecode:: python

    @app.timer(interval=60.0)
    async def every_minute():
        print('WAKE UP')


The above timer will print something every minute, starting from one minute
after the worker is started and fully operational.

.. _tasks-web-views:

Web Views
=========

.. _tasks-cli-commands:

CLI Commands
============

