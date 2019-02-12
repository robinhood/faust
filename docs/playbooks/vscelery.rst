.. _guide-vscelery:

=============================
 Overview: Faust vs. Celery
=============================

Faust is a stream processor, so what does it have in common with Celery?

If you've used tools such as Celery in the past, you can think of Faust as being able
to, not only run tasks, but for tasks to keep history of everything that has
happened so far. That is tasks ("agents" in Faust) can keep state, and also
replicate that state to a cluster of Faust worker instances.

If you have used :pypi:`Celery` you probably know tasks such as this:

.. sourcecode:: python

    from celery import Celery

    app = Celery(broker='amqp://')

    @app.task()
    def add(x, y):
        return x + y

    if __name__ == '__main__':
        add.delay(2, 2)

Faust uses Kafka as a broker, not RabbitMQ, and Kafka behaves differently
from the queues you may know from brokers using AMQP/Redis/Amazon SQS/and so on.

Kafka doesn't have queues, instead it has "topics" that can work
pretty much the same way as queues. A topic is a log structure
so you can go forwards and backwards in time to retrieve the history
of messages sent.

The Celery task above can be rewritten in Faust like this:

.. sourcecode:: python

    import faust

    app = faust.App('myapp', broker='kafka://')

    class AddOperation(faust.Record):
        x: int
        y: int

    @app.agent()
    async def add(stream):
        async for op in stream:
            yield op.x + op.y

    @app.command()
    async def produce():
        await add.send(value=AddOperation(2, 2))

    if __name__ == '__main__':
        app.main()

Faust also support storing state with the task (see :ref:`guide-tables`),
and it supports leader election which is useful for things such as locks.

**Learn more about Faust in the** :ref:`introduction` **introduction page**
    to read more about Faust, system requirements, installation instructions,
    community resources, and more.

**or go directly to the** :ref:`quickstart` **tutorial**
    to see Faust in action by programming a streaming application.

**then explore the** :ref:`User Guide <guide>`
    for in-depth information organized by topic.
