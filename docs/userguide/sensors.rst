.. _guide-sensors:

=====================================
 Sensors - Monitors and Statistics
=====================================

.. contents::
    :local:
    :depth: 1

.. module:: Faust

.. currentmodule:: faust

.. _sensor-basics:

Basics
======

Sensors record information about events in a Faust application
as they happen.

You can define custom sensors to record information that you care about,
just add it to the list of application sensors. There's also a default
sensor called the "monitor" that record the runtime of messages and events
as they go through the worker, the latency of publishing messages,
the latency of committing Kafka offsets, and so on.

The web server uses this monitor to present graphs and statistics about
your instance, and there's also a versions of the monitor available that
forwards statistics to `StatsD`_, and `Datadog`_.

.. _`StatsD`: https://github.com/etsy/statsd
.. _`Datadog`: https://www.datadoghq.com

.. _sensor-monitor:

Monitor
=======

The :class:`faust.Monitor` is a built-in sensor that captures information like:

* Average message processing time (when all agents have processed a message).

* Average event processing time (from an event received by an agent to
  the event is :term:`acked`.)

* The total number of events processed every second.

* The total number of events processed every second listed by topic.

* The total number of events processed every second listed by agent.

* The total number of records written to tables.

* Duration of Kafka topic commit operations (latency).

* Duration of producing messages (latency).

You can access the state of the monitor, while the worker is running,
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
build custom sensors.

Methods
-------

.. _sensor-message:

Message Callbacks
-----------------

.. class:: Sensor
    :noindex:

        .. automethod:: on_message_in
            :noindex:

        .. automethod:: on_message_out
            :noindex:

.. _sensor-event:

Event Callbacks
---------------

.. class:: Sensor
    :noindex:

        .. automethod:: on_stream_event_in
            :noindex:

        .. automethod:: on_stream_event_out
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

        .. automethod:: on_commit_initiated
            :noindex:

        .. automethod:: on_commit_completed
            :noindex:

        .. automethod:: on_send_initiated
            :noindex:

        .. automethod:: on_send_completed
            :noindex:
