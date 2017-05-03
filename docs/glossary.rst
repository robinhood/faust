.. _glossary:

Glossary
========

.. glossary::
    :sorted:

    acked
        Short for :term:`acknowledged`

    acknowledged
        A message is acknowledged once the message is fully processed.  It's
        a signal that the program does not want to see the message again.
        Faust will advance the offset, and commit, only after a message has
        been acknowledged.

    codec
        A codec encodes/decodes data to some format or encoding.
        Examples of codecs includes Base64 encoding, json serialization,
        pickle serialization, text encoding conversion, etc.

    consumer
        A process receiving messages.

    message
        The unit of data published to, or received from the message transport.
        A message has a key and a value and is sent to a topic.

    publisher
        A process sending messages.

    sensor
        A sensor is called when events happen in Faust and can record
        statistics.

    serializer
        A serializer is a combination of :term:`codecs <codec>`,
        and is responsibile for serializing keys and
        values in messages so that they can be sent over the network.

    S-routine
        An S-routine is like a coroutine but instead of `coro.send()`
        it communicates via an async iterable::

            async def sroutine(inbox):
                return (process(message) async for message in inbox)

    topic
        Messages are sent to topics, and consumers subscribe to topics of
        interest.

    task
        A function that starts and operates on one or more streams.
        Since streams are infinite a task will usually not end unless
        the program is shut down.

        A task is also a unit for concurrency, so a task can not execute
        on multiple threads.

    thread safe
        A function or process that is thread safe means that multiple POSIX
        threads can execute it in parallel without race conditions or deadlock
        situations.

    transport
        A communication channel used to send and receive messages, e.g. Kafka.

