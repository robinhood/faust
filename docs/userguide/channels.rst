.. _guide-channels:

=====================================
 Channels & Topics - Data Sources
=====================================

.. contents::
    :local:
    :depth: 1

.. module:: faust

.. currentmodule:: faust

Basics
======

Faust agents iterate over streams, and streams iterate over channels.

A channel is a construct used to send and receive messages,
then we have the "topic", which is a named-channel backed by a Kafka topic.

.. topic:: \

    Streams read from channels (either a local-channel or a topic).

    ``Agent`` <--> ``Stream`` <--> ``Channel``

    Topics are named-channels backed by a transport (to use e.g. Kafka topics):

    ``Agent`` <--> ``Stream`` <--> ``Topic`` <--> ``Transport`` <--> :pypi:`aiokafka`

Faust defines these layers of abstraction so that agents can send and
receive messages using more than one type of transport.

Topics are highly Kafka specific, while channels are not. That makes
channels more natural to subclass should you require a different
means of communication, for example using `RabbitMQ`_ (AMQP),
`Stomp`_, `MQTT`_, `NSQ`_, `ZeroMQ`_, etc.

.. _`RabbitMQ`: http://rabbitmq.com/
.. _`STOMP`: https://stomp.github.io/
.. _`MQTT`: http://mqtt.org/
.. _`NSQ`: http://nsq.io/
.. _`ZeroMQ`: http://zeromq.org/

Channels
========

A **channel** is a buffer/queue used to send and receive messages.
This buffer could exist in-memory in the local process only,
or transmit serialized messages over the network.

You can create channels manually and read/write from them:

.. sourcecode:: python

    async def main():
        channel = app.channel()

        await channel.put(1)

        async for event in channel:
            print(event.value)
            # the channel is infinite so we break after first event
            break

Reference
~~~~~~~~~

Sending messages to channel
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. class:: Channel
    :noindex:

    .. autocomethod:: send
        :noindex:

    .. automethod:: as_future_message
        :noindex:

    .. autocomethod:: publish_message
        :noindex:


Declaring
^^^^^^^^^

.. note::

    Some channels may require you to declare them on the server side
    before they're used. Faust will create topics considered internal but
    will not create or modify "source topics" (i.e., exposed for use by
    other Kafka applications).

    To define a topic as internal use
    ``app.topic('name', ..., internal=True)``.

.. class:: Channel
    :noindex:

    .. autocomethod:: maybe_declare
        :noindex:

    .. autocomethod:: declare
        :noindex:

Topics
======

A *topic* is a **named channel**, backed by a Kafka topic. The name is used as the address
of the channel, to share it between multiple processes and each
process will receive a partition of the topic.
