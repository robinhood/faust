.. _guide-sensors:

=====================================
 Sensors - Monitors and Statistics
=====================================

.. contents::
    :local:
    :depth: 1

.. module:: faust
    :noindex:

.. currentmodule:: faust

.. _sensor-basics:

Basics
======

Sensors record information about events occurring in a Faust application
as they happen.

There's a default sensor called "the monitor" that record the
runtime of messages and events as they go through the worker,
the latency of publishing messages, the latency of committing Kafka
offsets, and so on.

The web server uses this monitor to present graphs and statistics about
your instance, and there's also a versions of the monitor available that
forwards statistics to `StatsD`_, and `Datadog`_.

You can define custom sensors to record the information that you care about,
and enable them in the worker.


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

        .. autoattribute:: send_errors
            :annotation:
            :noindex:

        .. autoattribute:: messages_sent_by_topic
            :annotation:
            :noindex:

        .. autoattribute:: topic_buffer_full
            :annotation:
            :noindex:

        .. autoattribute:: metric_counts
            :annotation:
            :noindex:

        .. autoattribute:: tp_committed_offsets
            :annotation:
            :noindex:

        .. autoattribute:: tp_read_offsets
            :annotation:
            :noindex:

        .. autoattribute:: tp_end_offsets
            :annotation:
            :noindex:

        .. autoattribute:: assignment_latency
            :annotation:
            :noindex:

        .. autoattribute:: assignments_completed
            :annotation:
            :noindex:

        .. autoattribute:: assignments_failed
            :annotation:
            :noindex:

        .. autoattribute:: rebalances
            :annotation:
            :noindex:

        .. autoattribute:: rebalance_return_latency
            :annotation:
            :noindex:

        .. autoattribute:: rebalance_end_latency
            :annotation:
            :noindex:

        .. autoattribute:: rebalance_return_avg
            :annotation:
            :noindex:

        .. autoattribute:: rebalance_end_avg
            :annotation:
            :noindex:

        .. autoattribute:: http_response_codes
            :annotation:
            :noindex:

        .. autoattribute:: http_response_latency
            :annotation:
            :noindex:

        .. autoattribute:: http_response_latency_avg
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

Consumer Callbacks
------------------

.. class:: Sensor
    :noindex:

    .. automethod:: on_commit_initiated
        :noindex:

    .. automethod:: on_commit_completed
        :noindex:

    .. automethod:: on_topic_buffer_full
        :noindex:

    .. automethod:: on_assignment_start
        :noindex:

    .. automethod:: on_assignment_error
        :noindex:

    .. automethod:: on_assignment_completed
        :noindex:

    .. automethod:: on_rebalance_start
        :noindex:

    .. automethod:: on_rebalance_return
        :noindex:

    .. automethod:: on_rebalance_end
        :noindex:

Producer Callbacks
------------------

.. class:: Sensor
    :noindex:

    .. automethod:: on_send_initiated
        :noindex:

    .. automethod:: on_send_completed
        :noindex:

    .. automethod:: on_send_error
        :noindex:

Web Callbacks
-------------

.. class:: Sensor
    :noindex:

    .. automethod:: on_web_request_start
        :noindex:

    .. automethod:: on_web_request_end
        :noindex:
