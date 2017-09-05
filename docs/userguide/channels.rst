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

A *channel* is a buffer/queue used to send and receive messages,
where this buffer can be in-memory, an IPC construct, or transmit
serialized messages over the network.

A *topic* is a named channel, that is the name is used as the address
for the channel, and two topics using the same name will be backed using
the same Kafka topic.


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
