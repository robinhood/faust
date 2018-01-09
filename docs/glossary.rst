.. _glossary:

Glossary
========

.. glossary::
    :sorted:

    acked
        Short for :term:`acknowledged`.

    acknowledged
        Acknowledgement marks a message as fully processed.
        It’s a signal that the program does not want to see the message again.
        Faust advances the offset by committing after a message is acknowledged.

    codec
        A codec encodes/decodes data to some format or encoding.
        Examples of codecs include Base64 encoding, JSON serialization,
        pickle serialization, text encoding conversion, and more.

    consumer
        A process that receives messages.

    message
        The unit of data published or received from the message transport.
        A message has a key and a value.

    publisher
        A process sending messages.

    sensor
        A sensor records information about events happening in a running
        Faust application.

    serializer
        A serializer is a :term:`codecs <codec>`, responsible for serializing
        keys and values in messages sent over the network.

    topic
        Consumers subscribe to topics of interest, and producers send messages
        to consumers via the topic.

    agent
        An async function that operates on on a stream.
        Since streams are infinite the agent will usually not end unless
        the program is shut down.

    task
        A task is the unit of concurrency in an :mod:`asyncio` program.

    thread safe
        A function or process that is thread safe means that multiple POSIX
        threads can execute it in parallel without race conditions or deadlock
        situations.

    transport
        A communication mechanism used to send and receive messages, for
        example Kafka.

