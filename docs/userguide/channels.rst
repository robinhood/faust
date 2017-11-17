.. _guide-channels:

=====================================
 Channels & Topics
=====================================

.. contents::
    :local:
    :depth: 1

.. module:: faust

.. currentmodule:: faust

Basics
======

Channels are what Faust agents (stream processors) read from.
You don't need to know how channels work to use Faust, as agents
work with streams, not a channel directly.

`Agent` <--> `Stream` <--> `Channel` <--> `Transport` <--> :pypi:`aiokafka`

The agent reads from the stream, the stream reads events from a channel,
and the channel is populated with messages from a message transport,
where the message transport may range from everything from in-memory (pure
channels), or to reading from a Kafka topic, using ``app.topic(name)`` which
is also a type of channel.

These are all just layers of abstraction used so
that agents can send and receive messages using more than one type of
transport.  The Faust ``Transport`` class is highly Kafka specific, but
channels are not, and that makes them easier to subclass if you require
a different type of channel, for example using `AMQP`_, `Stomp`_, `MQTT`_,
etc., instead of Kafka as the message transport.

Channels
========

A *channel* is a buffer/queue used to send and receive messages,
where this buffer can be in-memory, an IPC construct, or transmit
serialized messages over the network.

You can create channels manually and read/write from them:

.. sourcecode:: python

    async def main():
        channel = app.channel()

        await channel.put(1)

        async for event in channel:
            print(event.value)

Topics
======

A *topic* is a **named channel**, backed by a Kafka topic. The name is used as the address
of the channel, that way it can be shared by multiple processes and each
process will receive a partition of the topic.

Reference
~~~~~~~~~

Serialization/Deserialization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. class:: Record
    :noindex:

    .. automethod:: loads
        :noindex:

    .. automethod:: dumps
        :noindex:

    .. automethod:: to_representation
        :noindex:

Schemas
^^^^^^^

.. class:: Record
    :noindex:

    .. automethod:: as_schema
        :noindex:

    .. automethod:: as_avro_schema
        :noindex:
