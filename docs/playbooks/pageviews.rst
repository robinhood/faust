.. _playbooks-pageviews:

============================================================
  Tutorial: Count page views
============================================================

.. contents::
    :local:
    :depth: 2

In the :ref:`quickstart` tutorial, we went over a simple example
reading through a stream of greetings and printing them to the console.
In this playbook we do something more meaningful with an incoming stream,
we'll maintain real-time counts of page views from a stream of page views.

Application
-----------

As we did in the :ref:`quickstart` tutorial, we first define our application.
Let's create the module :file:`page_views.py`:

.. sourcecode:: python

    import faust

    app = faust.App(
        'page_views',
        broker='kafka://localhost:9092',
        topic_partitions=4,
    )

The :setting:`topic_partitions` setting defines the maximum number
of workers we can distribute the workload to (also sometimes referred as
the "sharding factor"). In this example, we set this to 4, but in a
production app, we ideally use a higher number.

Page View
----------

Let's now define a :ref:`model <guide-models>` that each page view event
from the stream deserializes into. The record is used for JSON dictionaries
and describes fields much like the new dataclasses in Python 3.7:

Create a model for our page view event:

.. sourcecode:: python

    class PageView(faust.Record):
        id: str
        user: str

Type annotations are used not only for defining static types, but also
to define how fields are deserialized, and lets you specify models
that contains other models, and so on.  See the :ref:`guide-models` guide
for more information.

Input Stream
------------

Next we define the source topic to read the "page view" events from,
and we specify that every value in this topic is of the PageView type.

.. sourcecode:: python

    page_view_topic = app.topic('page_views', value_type=PageView)

Counts Table
------------

Then we define a :ref:`Table <guide-tables>`. This is like a Python
dictionary, but is distributed across the cluster, partitioned by the
dictionary key.

.. sourcecode:: python

    page_views = app.Table('page_views', default=int)

Count Page Views
----------------

Now that we have defined our input stream, as well as a table to maintain
counts, we define an agent reading each page view event coming into the
stream, always incrementing the count for that page in the table.

Create the agent:

.. sourcecode:: python

    @app.agent(page_view_topic)
    async def count_page_views(views):
        async for view in views.group_by(PageView.id):
            page_views[view.id] += 1

.. note::

    Here we use :class:`~@Stream.group_by` to repartition the input stream by
    the page id. This is so that we maintain counts on each instance sharded
    by the page id. This way in the case of failure, when we move the
    processing of some partition to another node, the counts for that
    partition (hence, those page ids) also move together.

Now that we written our project, let's try running it to see the counts
update in the changelog topic for the table.

Starting Kafka
--------------

Before starting a worker, you need to start Zookeeper and Kafka.

First start Zookeeper:

.. sourcecode:: console

    $ $KAFKA_HOME/bin/zookeeper-server-start $KAFKA_HOME/etc/kafka/zookeeper.properties

Then start Kafka:

.. sourcecode:: console

    $ $KAFKA_HOME/bin/kafka-server-start $KAFKA_HOME/etc/kafka/server.properties

Starting the Faust worker
-------------------------

Start the worker, similar to what we did in the :ref:`quickstart` tutorial:

.. sourcecode:: console

    $ faust -A page_views worker -l info

Seeing it in action
-------------------

Now let's produce some fake page views to see things in action. Send
this data to the ``page_views`` topic:

.. sourcecode:: console

    $ faust -A page_views send page_views '{"id": "foo", "user": "bar"}'

Look at the changelog topic to see the counts. To look at the
changelog topic we will use the Kafka console consumer.

.. sourcecode:: console

    $ $KAFKA_HOME/bin/kafka-console-consumer --topic page_views-page_views-changelog --bootstrap-server localhost:9092 --property print.key=True --from-beginning

.. note::

    By default the changelog topic for a given ``Table`` has the format
    ``<app_id>-<table_name>-changelog``
