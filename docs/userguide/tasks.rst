.. _guide-tasks:

==========================================
 Tasks
==========================================

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

    log_topic = faust.topic('logs', value_type=LogRecord)

    app = faust.App('logs', url='aiokafka://localhost:9092')
    stats = Stats()


    @app.task
    async def process_logs(app):
        async for log in app.stream(log_topic):
            state.logs_received += 1
            if log.severity == 'ERROR':
                print('ERROR: {}'.format(log.message))


    @app.task
    async def dumps_stats(app):
        while 1:
            await asyncio.sleep(30.0)
            print(f'Logs processed: {stats.logs_received})

    if __name__ == '__main__':
        app.start()

.. _task-starting:

Starting tasks
==============

Tasks can be registered with an app in two ways:

1) Using the ``@app.task`` decorator

2) Manually using the ``app.add_task()`` method.

But this is just a best practice, as any asyncio Task will be allowed to
iterate over streams.  Explicitly defining what are Faust tasks
aids introspection, which may be used for debugging and monitoring
purposes.
