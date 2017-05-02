.. _intro:

=============================
 Introduction to Faust
=============================

.. contents::
    :local:
    :depth: 1

What is Stream Processing?
==========================

.. topic:: Faust is...

    - **Simple**

        Faust is extremely easy to use compared to other stream processing
        solutions.  There's no DSL to limit your creativity, no restricted
        set of operations to work from, and since Faust is a library it can
        integrate with just about anything.  All you need is a Kafka broker.

        Here's one of the simplest applications you can make:

        .. code-block:: python

            import faust

            app = faust.App('hello-app', url='kafka://localhost')

            class Greeting(faust.Record):
                from: str
                to: str

            topic = faust.topic('hello-topic', value_serializer=Greeting)

            @app.task
            async def hello(app):
                async for greeting in app.stream(topic):
                    print(f'Hello from {greeting.from} to {greeting.to}')

            @app.timer(interval=1.0)
            async def example_sender(app):
                await app.send(topic, value=Greeting(from='Faust', to='you'))

            if __name__ == '__main__':
                app.run()

        You're probably a bit intimidated by the `async` and `await` keywords,
        but you don't have to know how asyncio works to use
        Faust: just mimic the examples and you'll be fine.

        The example application starts two tasks: one is processing a stream,
        the other is a background thread sending events to that stream.
        In a real-live application your system will publish
        events to Kafka topics that your processors can consume from,
        and the background thread is only needed to feed data into our
        example.

    - **Highly Available**

        Faust is highly available and can survive network problems and server
        crashes.  In the case of node failure it can automatically recover,
        and tables have standby nodes that will take over.

    - **Scalable**

        Start more instances of your application as needed.

    - **Fast**

        Faust applications can hopefully handle millions of events per second
        in the future.

    - **Flexible**

        Faust is just Python, and a stream is just an infinite async iterator.
        If you know how to use Python, you already know how to use Faust,
        and it works with your favorite Python libraries like Django, Flask,
        SQLAlchemy, NTLK, NumPy, Scikit, TensorFlow, etc.


.. topic:: Faust can be used for...

    .. hlist::
        :columns: 2

        - **Event processing**

        - **Distributed joins and aggregations**

        - **Machine learning**

        - **Asynchronous tasks**

        - **Distributed computing**

        - **Data warehousing**

        - **Intrusion detection**

        - **and more...**

What do I need?
===============

.. sidebar:: Version Requirements
    :subtitle: Faust version 1.0 runs on

    - Python 3.6
    - Kafka 0.10 or later.

Faust requires Python 3.6 or later, and a running Kafka broker.

There's currently no plan to port Faust to earlier Python versions,
please contact the project if this is something that you'd like to work on.

How do I use it?
================

.. topic:: Step 1: Add events to your system

    Was an account created? Publish to Kafka.
    Did someone change their password? Publish to Kafka.
    Did someone make an order, create a comment, tag something, ...? Publish
    it all to Kafka!

.. topic:: Step 2: Use Faust to process those events

    Some ideas based around the events mentioned above:

    - Send email once an order is dispatched.

    - Find orders that were made, but no associated dispatch event
      after three days.

    - Find accounts that changed their password from a suspicious IP address.

    - Starting to get the idea?
