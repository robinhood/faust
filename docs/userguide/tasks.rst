.. _guide-tasks:

==========================================
 Tasks
==========================================

.. contents::
    :local:
    :depth: 1

.. module:: faust

.. currentmodule:: faust

.. _task-basics:

Basics
======

A task in Faust is simply an async function iterating over a stream,
but more than that it builds on the concept of a task in ``asyncio``,
so any async callable can act as a task.  This is useful if you want
your application to have background tasks that do not directly consume a
stream, like periodic timers.

Here is an example Faust app consuming log messages, and also emitting
statistics every 30 seconds:

.. code-block:: python

    import faust


    class LogRecord(faust.Record):
        severity: str
        message: str


    class Stats:
        logs_received = 0


    app = faust.App('logs', url='aiokafka://localhost:9092')
    log_topic = app.topic('logs', value_type=LogRecord)
    stats = Stats()


    @app.actor(log_topic)
    async def process_logs(logs):
        async for log in logs:
            state.logs_received += 1
            if log.severity == 'ERROR':
                print(f'ERROR: {log.message}')


    @app.timer(interval=30.0)
    async def dumps_stats():
        print(f'Logs processed: {stats.logs_received})

    if __name__ == '__main__':
        app.start_worker()

Actors
======

Concurrency
-----------

For idempotent, stateless tasks you may use the ``concurrency`` argument to
start multiple instances of the same task:

.. code-block:: python

    @app.actor(feed_topics, concurrency=100)
    async def import_feeds(feeds):
        async for feed in feeds:
            await import_feed(feed)

Timers
======

A shortcut decorator is included for starting background tasks that perform
some action at regular intervals.

.. code-block:: python

    @app.timer(interval=30.0)
    def dump_stats():
        print(f'Logs processed: {stats.logs_received})
