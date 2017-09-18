.. _playbooks-pageviews:

============================================================
  Count Page Views
============================================================

.. contents::
    :local:
    :depth: 2

In the :ref:`guide-quickstart` we went over a simple example where
we read through a stream of greetings and printed them to the console. In
this playbook we will do something more meaningful with an incoming stream.
We will maintain real-time counts of page views from a stream of page views.

Application
-----------

As we did in the :ref:`guide-quickstart`, we first define our application.
Let us create a module ``page_views.py`` and define the application:

.. sourcecode:: python

    import faust

    app = faust.App(
        'page_views',
        url='kafka://localhost:9092',
        default_partitions=4,
    )

The ``default_partitions`` parameter defines the maximum number of workers we
could distribute the workload of the application (also sometimes referred as
the sharding factor of the application). In this example we have set this to
4, in a production app with high throughput we would ideally set a higher
value for ``default_partition``.

Page View
----------

Let us now define the model for a page view event. Each page view event
from the stream of page view events would be deserialized into this
:ref:`model <guide-models>`.

.. sourcecode:: python

    class PageView(faust.Record):
        id: str
        user: str

Input Stream
------------

Now we define the input stream to read the page view events from.

.. sourcecode:: python

    page_view_topic = app.topic('page_views')

Counts Table
------------

Now we define a :ref:`Table <guide-tables>` to maintain counts for page views.

.. sourcecode:: python

    page_views = app.Table('page_views', default=int)

Count Page Views
----------------

Now that we have defined our input stream as well as a table to maintain
counts, we define an actor that would read each page view event coming in the
stream and actually do the counting.

.. sourcecode:: python

    @app.actor(page_view_topic)
    async def count_page_views(views):
        async for view in views.group_by(PageView.id):
            page_views[view.id] += 1

.. note::

    Here we use :class:`~@Stream.group_by` to repartition the input stream by
    the page id. This is so that we maintain counts on each instance sharded
    by the page id. This way in the case of failure, when we move the
    processing of some partition to another node, the counts for that
    partition (hence, those page ids) also move together.

Now we have our basic application working. Lets try running this as is and
see the counts being updated in the changelog topic for the table defined above.

Starting Kafka
--------------

You first need to start Kafka before running your first app that you wrote
above.

For Kafka, you first need to start Zookeeper:

.. sourcecode:: console

    $ $KAFKA_HOME/bin/zookeeper-server-start $KAFKA_HOME/etc/kafka/zookeeper.properties

Next, start Kafka:

.. sourcecode:: console

    $ $KAFKA_HOME/bin/kafka-server-start $KAFKA_HOME/etc/kafka/server.properties


Running the Faust worker
------------------------

As in the :ref:`guide-quickstart` start the application as follows:

.. sourcecode:: console

    $ faust -A page_views worker -l info

Seeing things in Action
-----------------------

Now let us produce some fake page views to see things in action. Let us
directly send these views to the topic ``page_views`` defined above.

.. sourcecode:: console

    $ faust -A page_views send page_views '{"id": "foo", "user": "bar"}'

Now let us look at the changelog topic to see the counts. To look at the
changelog topic we will use the kafka console consumer.

.. sourcecode:: console

    $ $KAFKA_HOME/bin/kafka-console-consumer --topic page_views-page_views-changelog --bootstrap-server localhost:9092 --property print.key=True --from-beginning

.. note::

    By default the changelog topic for a given ``Table`` has the format
    ``<app_id>-<table_name>-changelog``

