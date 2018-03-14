.. _glossary:

Glossary
========

.. glossary::
    :sorted:

    acked
    acking
    acknowledged
        Acknowledgement marks a message as fully processed.
        It’s a signal that the program does not want to see the message again.
        Faust advances the offset by committing after a message is acknowledged.

    codec
        A codec encodes/decodes data to some format or encoding.
        Examples of codecs include Base64 encoding, JSON serialization,
        pickle serialization, text encoding conversion, and more.

    consumer
        A process that receives messages from a broker, or a process
        that is actively reading from a topic/channel.

    message
        The unit of data published or received from the
        message :term:`transport`.  A message has a key and a value.

    publisher
        A process sending messages, or a process publishing data to a topic.

    sensor
        A sensor records information about events happening in a running
        Faust application.

    serializer
        A serializer is a :term:`codec`, responsible for serializing
        keys and values in messages sent over the network.

    topic
        Consumers subscribe to topics of interest, and producers send messages
        to consumers via the topic.

    agent
        An async function that iterates over a stream.
        Since streams are infinite the agent will usually not end unless
        the program is shut down.

    task
        A task is the unit of :term:`concurrency` in an :mod:`asyncio` program.

    concurrent
    concurrency
        A concurrent process can deal with many things at once, but not
        necessarily execute them in :term:`parallel`.  For example a web
        crawler may have to fetch thousands of web pages, and can work on them
        concurrently.

        This is distinct from :term:`parallelism` in that the process
        will switch between fetching web pages, but not actually process
        any of them at the same time.

    parallel
    parallelism
        A parallel process can execute many things at the same time,
        which will usually require running on multiple CPU cores.

        In contrast the term :term:`concurrency` refers to something
        that is seemingly parallel, but does not actually execute at the same
        time.

    thread safe
        A function or process that is thread safe means multiple POSIX
        threads can execute it in parallel without race conditions or deadlock
        situations.

    transport
        A communication mechanism used to send and receive messages, for
        example Kafka.

    event
        A happening in a system, or in the case of a stream, a single record
        having a key/value pair, and a reference to the original message
        object.

    idempotence
    idempotent
    idempotency
        Idempotence is a mathematical property that describes a function that
        can be called multiple times without changing the result.
        Practically it means that a function can be repeated many times without
        unintended effects, but not necessarily side-effect free in the pure
        sense (compare to :term:`nullipotent`).

        Further reading: https://en.wikipedia.org/wiki/Idempotent

    nullipotent
    nillipotence
    nullipotency
        describes a function that'll have the same effect, and give the same
        result, even if called zero or multiple times (side-effect free).
        A stronger version of :term:`idempotent`.

    reentrant
    reentrancy
        describes a function that can be interrupted in the middle of
        execution (e.g., by hardware interrupt or signal), and then safely
        called again later. Reentrancy isn't the same as
        :term:`idempotence <idempotent>` as the return value doesn't have to
        be the same given the same inputs, and a reentrant function may have
        side effects as long as it can be interrupted;  An idempotent function
        is always reentrant, but the reverse may not be true.
