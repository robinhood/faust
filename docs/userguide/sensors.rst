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

.. sourcecode:: python

    @app.agent(app.topic('topic'))
    def mytask(events):
        async for event in events:
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
            :annotation:
            :noindex:

        .. autoattribute:: messages_active
            :annotation:
            :noindex:

        .. autoattribute:: messages_received_total
            :annotation:
            :noindex:

        .. autoattribute:: messages_received_by_topic
            :annotation:
            :noindex:

        .. autoattribute:: messages_s
            :annotation:
            :noindex:

        .. autoattribute:: events_active
            :annotation:
            :noindex:

        .. autoattribute:: events_total
            :annotation:
            :noindex:

        .. autoattribute:: events_s
            :annotation:
            :noindex:

        .. autoattribute:: events_by_stream
            :annotation:
            :noindex:

        .. autoattribute:: events_by_task
            :annotation:
            :noindex:

        .. autoattribute:: events_runtime
            :annotation:
            :noindex:

        .. autoattribute:: events_runtime_avg
            :annotation:
            :noindex:

        .. autoattribute:: tables
            :annotation:
            :noindex:

        .. autoattribute:: commit_latency
            :annotation:
            :noindex:

        .. autoattribute:: send_latency
            :annotation:
            :noindex:

        .. autoattribute:: messages_sent
            :annotation:
            :noindex:

        .. autoattribute:: messages_sent_by_topic
            :annotation:
            :noindex:

.. _monitor-configuration:

Configuration Attributes
^^^^^^^^^^^^^^^^^^^^^^^^

.. class:: Monitor
    :noindex:

        .. autoattribute:: max_avg_history
            :noindex:

        .. autoattribute:: max_commit_latency_history
            :noindex:

        .. autoattribute:: max_send_latency_history
            :noindex:

.. _monitor-tablestate:

Class: ``TableState``
~~~~~~~~~~~~~~~~~~~~~

.. class:: faust.sensors.TableState
    :noindex:

        .. autoattribute:: table
            :noindex:

        .. autoattribute:: keys_retrieved
            :noindex:

        .. autoattribute:: keys_updated
            :noindex:

        .. autoattribute:: keys_deleted
            :noindex:

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

        .. autocomethod:: on_message_in
            :noindex:

        .. autocomethod:: on_message_out
            :noindex:

.. _sensor-event:

Event Callbacks
---------------

.. class:: Sensor
    :noindex:

        .. autocomethod:: on_stream_event_in
            :noindex:

        .. autocomethod:: on_stream_event_out
            :noindex:

.. _sensor-table:

Table Callbacks
---------------

.. class:: Sensor
    :noindex:

        .. automethod:: on_table_get
            :noindex:

        .. automethod:: on_table_set
            :noindex:

        .. automethod:: on_table_del
            :noindex:

.. _sensor-operations:

Operation Callbacks
-------------------

.. class:: Sensor
    :noindex:

        .. autocomethod:: on_commit_initiated
            :noindex:

        .. autocomethod:: on_commit_completed
            :noindex:

        .. autocomethod:: on_send_initiated
            :noindex:

        .. autocomethod:: on_send_completed
            :noindex:
