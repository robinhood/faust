.. _guide-sensors:

=====================================
 Sensors
=====================================

.. contents::
    :local:
    :depth: 1

.. module:: Faust

.. currentmodule:: faust

.. _sensor-basics:

Basics
======

Sensors record information about events happening inside of a running Faust
application.

The default sensor will keep track of messages, and events as they go into the
system, but also record the latency of sending messages, committing the Kafka
offset, and so on.

You can also create your own sensors to record additional information.

.. _sensor-monitor:

Monitor
=======

The :class:`faust.Monitor` is a built-in sensor that captures information like:

* Total number of events

* Average processing time (from event received to event acked)

* number of events processed/s

* Number of events processed/s by topic

* Number of events processed/s by task

* Number of records written to tables.

* How long it takes to commit messages.

* How long it takes to send messages.

When the Faust application is running you can access the state of this monitor
in ``app.monitor``:

.. code-block:: python

    @app.task
    def mytask(app):
        async for event in app.stream(faust.topic('topic')):
            # emit how many events are being processed every second.
            print(app.monitor.events_s)

.. _monitor-reference:

Monitor API Reference
---------------------

Class: ``Monitor``
~~~~~~~~~~~~~~~~~~

.. _monitor-message:

Monitor Attributes
^^^^^^^^^^^^^^^^^^

.. class:: Monitor
    :noindex:

        .. autoattribute:: messages

        .. autoattribute:: messages_active

        .. autoattribute:: messages_received_total

        .. autoattribute:: messages_received_by_topic

        .. autoattribute:: messages_s

        .. autoattribute:: events_active

        .. autoattribute:: events_total

        .. autoattribute:: events_s

        .. autoattribute:: events_by_stream

        .. autoattribute:: events_by_task

        .. autoattribute:: events_runtime

        .. autoattribute:: events_runtime_avg

        .. autoattribute:: tables

        .. autoattribute:: commit_latency

        .. autoattribute:: send_latency

        .. autoattribute:: messages_sent

        .. autoattribute:: messages_sent_by_topic

.. _monitor-configuration:

Configuration Attributes
^^^^^^^^^^^^^^^^^^^^^^^^

.. class:: Monitor
    :noindex:

        .. autoattribute:: max_messages

        .. autoattribute:: max_avg_history

        .. autoattribute:: max_commit_latency_history

        .. autoattribute:: max_send_latency_history

.. _monitor-messagestate:

Class: ``MessageState``
~~~~~~~~~~~~~~~~~~~~~~~

.. class:: faust.sensors.MessageState
    :noindex:

        .. autoattribute:: consumer_id

        .. autoattribute:: tp

        .. autoattribute:: offset

        .. autoattribute:: time_in

        .. autoattribute:: time_out

        .. autoattribute:: time_total

        .. autoattribute:: streams

.. _monitor-eventstate:

Class: ``EventState``
~~~~~~~~~~~~~~~~~~~~~

.. class:: faust.sensors.EventState
    :noindex:

        .. autoattribute:: stream

        .. autoattribute:: time_in

        .. autoattribute:: time_out

        .. autoattribute:: time_total

.. _monitor-tablestate:

Class: ``TableState``
~~~~~~~~~~~~~~~~~~~~~

.. class:: faust.sensors.TableState
    :noindex:

        .. autoattribute:: table

        .. autoattribute:: keys_retrieved

        .. autoattribute:: keys_updated

        .. autoattribute:: keys_deleted

.. _sensor-reference:

Sensor API Reference
====================

This reference describes the sensor interface and is useful when you want to
build your own sensors.

Methods
-------

.. _sensor-message:

Message Callbacks
-----------------

.. class:: Sensor
    :noindex:

        .. automethod:: on_message_in

        .. automethod:: on_message_out

.. _sensor-event:

Event Callbacks
---------------

.. class:: Sensor
    :noindex:

        .. automethod:: on_stream_event_in

        .. automethod:: on_stream_event_out

.. _sensor-table:

Table Callbacks
---------------

.. class:: Sensor
    :noindex:

        .. automethod:: on_table_get

        .. automethod:: on_table_set

        .. automethod:: on_table_del

.. _sensor-operations:

Operation Callbacks
-------------------

.. class:: Sensor
    :noindex:

        .. automethod:: on_commit_initiated

        .. automethod:: on_commit_completed

        .. automethod:: on_send_initiated

        .. automethod:: on_send_completed
